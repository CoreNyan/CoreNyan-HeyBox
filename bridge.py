"""Redis HeyBox adapter. No LLM, login automation, or import-time account/disk reads.

Writes submit once. An RPC id journals intent before submission; ambiguous results must
never be retried under a new id. Comment delivery is at-least-once until the consumer
persists its work and calls heybox_ack_comment_event. See docs/bridge.md.
"""

import asyncio
import dataclasses
import hashlib
import json
import logging
import math
import os
import sqlite3
import time
from contextvars import ContextVar
from datetime import date, datetime
from enum import Enum
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

from xhh_operator.errors import OperatorError
from xhh_operator.service import Operator, identifier
from xhh_operator.session import safe_result

logger = logging.getLogger("HeyBoxBridge")
ROOT = Path(__file__).resolve().parent
REDIS_URL = os.getenv("REDIS_URL")
REQ_CHANNEL = "corenyan:heybox:tool:request"
RES_CHANNEL = "corenyan:heybox:tool:response"
EVENT_CHANNEL = "corenyan:heybox:event:comment"
CONFIG_PATH = ROOT / "config.json"
PROCESSED_COMMENTS_FILE = ROOT / "processed_comments.json"
STATE_PATH = ROOT / "bridge_state.sqlite3"
operator: Operator | None = None
state = None
monitored_posts = set()
write_started = ContextVar("heybox_write_started", default=False)
WRITE_TOOLS = {
    "heybox_publish_post",
    "publish_post",
    "publish_article",
    "heybox_post_comment",
    "comment_post",
    "heybox_reply_comment",
    "reply_comment",
    "heybox_pin_comment",
    "pin_comment",
}


def json_value(value):
    """Explicit JSON conversion; do not stringify unknown objects or leak sessions."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return json_value(dataclasses.asdict(value))
    if callable(getattr(value, "model_dump", None)):
        return json_value(value.model_dump(mode="json"))
    if isinstance(value, Enum):
        return json_value(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return safe_result({k: json_value(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    raise TypeError("Non-JSON operator result")


def dumps(value):
    return json.dumps(json_value(value), ensure_ascii=False, allow_nan=False)


def load_config():
    try:
        cfg = json.loads(Path(CONFIG_PATH).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"poll_interval_seconds": 60, "monitored_posts": []}
    if not isinstance(cfg, dict):
        raise ValueError("Bridge config must be an object")
    return cfg


def redis_url():
    """Resolve deployment config explicitly; never silently downgrade authentication."""
    if os.getenv("REDIS_URL"):
        return os.environ["REDIS_URL"]
    cfg = load_config()
    if isinstance(cfg.get("redis_url"), str) and cfg["redis_url"]:
        return cfg["redis_url"]
    redis = cfg.get("redis")
    if isinstance(redis, dict):
        if isinstance(redis.get("url"), str) and redis["url"]:
            return redis["url"]
        from urllib.parse import quote

        host = redis.get("host")
        if isinstance(host, str) and host:
            username = quote(str(redis.get("username") or ""), safe="")
            password = quote(str(redis.get("password") or ""), safe="")
            auth = f"{username}:{password}@" if username or password else ""
            scheme = "rediss" if redis.get("ssl") is True else "redis"
            port, db = int(redis.get("port", 6379)), int(redis.get("db", 0))
            if not 1 <= port <= 65535 or db < 0:
                raise ValueError("Invalid Redis configuration")
            return f"{scheme}://{auth}{host}:{port}/{db}"
    raise ValueError("REDIS_URL or project redis_url/redis configuration is required")


class BridgeState:
    """SQLite durable outbox and RPC journal; errors fail closed, never reset state."""

    def __init__(self, path, legacy_path=None):
        self.db = sqlite3.connect(path)
        try:
            self._initialize(legacy_path)
        except BaseException:
            self.db.close()
            raise

    def _initialize(self, legacy_path):
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS pending (event_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS posts (post_id TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, response TEXT);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS legacy_seen (comment_id TEXT PRIMARY KEY);
        """)
        if not self.db.execute("SELECT 1 FROM meta WHERE key='legacy_imported'").fetchone():
            ids = []
            if legacy_path and Path(legacy_path).exists():
                ids = json.loads(Path(legacy_path).read_text(encoding="utf-8"))
                if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids):
                    raise ValueError("Invalid legacy comment state")
            with self.db:
                self.db.executemany(
                    "INSERT OR IGNORE INTO legacy_seen VALUES (?)", [(i,) for i in ids]
                )
                self.db.execute("INSERT INTO meta VALUES ('legacy_imported','1')")

    def close(self):
        self.db.close()

    def posts(self):
        return {row[0] for row in self.db.execute("SELECT post_id FROM posts ORDER BY post_id")}

    def add_post(self, post_id):
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO posts VALUES (?)", (identifier(post_id),))

    def enqueue(self, payload):
        event_id = payload["event_id"]
        with self.db:
            if self.db.execute("SELECT 1 FROM seen WHERE event_id=?", (event_id,)).fetchone():
                return
            if self.db.execute(
                "SELECT 1 FROM legacy_seen WHERE comment_id=?", (payload["comment_id"],)
            ).fetchone():
                return
            self.db.execute(
                "INSERT OR IGNORE INTO pending VALUES (?,?)", (event_id, dumps(payload))
            )

    def pending(self):
        return [
            json.loads(row[0])
            for row in self.db.execute("SELECT payload FROM pending ORDER BY rowid")
        ]

    def ack(self, event_id):
        with self.db:
            known = self.db.execute(
                "SELECT 1 FROM pending WHERE event_id=?", (event_id,)
            ).fetchone()
            seen = self.db.execute("SELECT 1 FROM seen WHERE event_id=?", (event_id,)).fetchone()
            if not known and not seen:
                raise ValueError("Unknown comment event id")
            self.db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (event_id,))
            self.db.execute("DELETE FROM pending WHERE event_id=?", (event_id,))
        return {"success": True, "event_id": event_id, "acknowledged": True}

    def begin_request(self, req_id, fingerprint):
        row = self.db.execute(
            "SELECT fingerprint,response FROM requests WHERE id=?", (req_id,)
        ).fetchone()
        if row:
            if row[0] != fingerprint:
                raise ValueError("RPC id reused with different arguments")
            return (
                json.loads(row[1])
                if row[1]
                else {
                    "id": req_id,
                    "success": False,
                    "data": {
                        "success": False,
                        "accepted": False,
                        "verified": False,
                        "status": "unknown",
                        "submission_state": "unknown",
                        "retry_safe": False,
                    },
                    "error": "Previous submission outcome is unknown; do not retry",
                }
            )
        with self.db:
            self.db.execute("INSERT INTO requests VALUES (?,?,NULL)", (req_id, fingerprint))
        return None

    def finish_request(self, req_id, response):
        with self.db:
            self.db.execute("UPDATE requests SET response=? WHERE id=?", (dumps(response), req_id))


def require_state():
    if state is None:
        raise RuntimeError("Bridge state is not initialized")
    return state


def post_id_arg(args):
    value = args.get("post_id") or args.get("link_id") or args.get("postId")
    if not value:
        raise ValueError("Missing post_id/link_id")
    return identifier(value)


def bounded_int(args, key, default, upper=100):
    value = args.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"Invalid {key}")
    number = int(value)
    if str(number) != str(value) or not 1 <= number <= upper:
        raise ValueError(f"Invalid {key}")
    return number


def boolean(args, key, default=False):
    value = args.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def text_arg(args, key):
    value = args.get(key, "")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing {key}")
    return value


def list_arg(args, key):
    value = args.get(key) or []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"{key} must be a string array")
    return value


def write_result(raw, id_key):
    raw = json_value(raw)
    if not isinstance(raw, dict):
        raise TypeError("Write result must be an object")
    accepted, verified = raw.get("accepted") is True, raw.get("verified") is True
    record_id = raw.get(id_key)
    success = bool(accepted and verified and record_id)
    return {
        **raw,
        "accepted": accepted,
        "verified": bool(success),
        "success": success,
        "status": "verified" if success else "unverified" if accepted else "unknown",
        "submission_state": "verified" if success else "accepted" if accepted else "unknown",
        "retry_safe": False,
    }


class WriteUncertain(Exception):
    pass


async def submit(call, id_key):
    write_started.set(True)
    try:
        return write_result(await call, id_key)
    except Exception as exc:
        raise WriteUncertain("Write outcome unknown; do not resubmit") from exc


async def find_comment(args):
    comment_id = identifier(text_arg(args, "comment_id"))
    if args.get("post_id") or args.get("link_id") or args.get("postId"):
        return await operator.find_comment(post_id_arg(args), comment_id)
    posts = sorted(monitored_posts | require_state().posts())
    configured = load_config().get("monitored_posts", [])
    posts = sorted(set(posts) | {identifier(p) for p in configured})
    if len(posts) > 100:
        raise ValueError("Too many monitored posts; supply post_id")
    matches = []
    for post_id in posts:
        try:
            matches.append(await operator.find_comment(post_id, comment_id))
        except OperatorError as exc:
            if exc.code != "target":
                raise
    if len(matches) != 1:
        raise ValueError("Comment not uniquely found in monitored posts; supply post_id")
    return {**matches[0], "scope": "monitored_posts"}


async def handle_tool(tool_name: str, args: dict[str, Any]):
    if load_config().get("enabled") is not True:
        raise ValueError("MODULE_DISABLED: heybox")
    if not isinstance(tool_name, str) or not isinstance(args, dict):
        raise ValueError("Invalid tool or args")
    tool_name = tool_name.lower().strip()
    if tool_name in WRITE_TOOLS:
        from sensitive_output import blocked_reason

        if blocked_reason(args):
            return {
                "success": False,
                "accepted": False,
                "verified": False,
                "status": "rejected",
                "error_code": "SENSITIVE_OUTPUT_BLOCKED",
                "submission_state": "not_submitted",
                "retry_safe": False,
            }
        # Image uploads require a separate reviewed workflow; text scanning cannot inspect pixels.
        if args.get("images"):
            return {
                "success": False,
                "accepted": False,
                "verified": False,
                "status": "rejected",
                "error_code": "UNREVIEWED_MEDIA_BLOCKED",
                "submission_state": "not_submitted",
                "retry_safe": False,
            }
    if tool_name == "heybox_ack_comment_event":
        return require_state().ack(text_arg(args, "event_id"))
    if tool_name in ("heybox_pin_comment", "pin_comment"):
        # No evidenced write endpoint exists in this Operator. Never guess one.
        return {
            "success": False,
            "accepted": False,
            "verified": False,
            "supported": False,
            "status": "unsupported",
            "error_code": "unsupported",
            "submission_state": "not_submitted",
            "retry_safe": False,
            "post_id": post_id_arg(args),
            "comment_id": identifier(text_arg(args, "comment_id")),
            "pinned": boolean(args, "pinned", True),
        }
    if operator is None:
        raise RuntimeError("Operator is not initialized")
    if tool_name in ("heybox_publish_post", "publish_post", "publish_article"):
        title, content = text_arg(args, "title"), text_arg(args, "content")
        communities = list_arg(args, "communities")
        communities = list(
            dict.fromkeys("Minecraft" if c in ("我的世界", "mc", "MC") else c for c in communities)
        )
        tags = list_arg(args, "tags") if "tags" in args else list_arg(args, "hashtags")
        images = list_arg(args, "images")
        draft = boolean(args, "draft")
        if len(communities) > 2 or len(tags) > 5:
            raise ValueError("Too many communities or hashtags")
        result = await submit(
            operator.write_post(
                title=title,
                content=content,
                post_kind="article",
                images=images,
                communities=communities,
                hashtags=tags,
                draft=draft,
                dry_run=False,
            ),
            "link_id",
        )
        if result.get("link_id") and result.get("accepted") and not draft:
            require_state().add_post(str(result["link_id"]))
            monitored_posts.add(str(result["link_id"]))
        result["url"] = (
            f"https://www.xiaoheihe.cn/link/{result['link_id']}" if result.get("link_id") else ""
        )
        return result
    if tool_name in ("heybox_get_post", "get_post"):
        return await operator.post(post_id_arg(args))
    if tool_name in ("heybox_get_comments", "get_comments"):
        return await operator.comments(
            post_id_arg(args),
            page_size=bounded_int(args, "limit", 20),
            max_pages=bounded_int(args, "max_pages", 100),
            allow_partial=True,
        )
    if tool_name in ("heybox_find_comment", "find_comment"):
        return await find_comment(args)
    if tool_name in (
        "heybox_post_comment",
        "comment_post",
        "heybox_reply_comment",
        "reply_comment",
    ):
        content, images = text_arg(args, "content"), list_arg(args, "images")
        reply_id = args.get("comment_id") or args.get("reply_id")
        # Core packets carry target_comment as approval context alongside exact IDs.
        # Only target_text explicitly requests text-based location; IDs take priority
        # over the legacy target_comment preview, never over a conflicting locator.
        if reply_id and args.get("target_text"):
            raise ValueError("Use comment_id or target_text, not both")
        target = None if reply_id else args.get("target_text") or args.get("target_comment")
        # The old root-comment sentinel remains supported, but is not authored copy.
        if target == "主楼评论区互动":
            target = None
        kwargs = {"images": images, "dry_run": False}
        if reply_id:
            found = await find_comment({**args, "comment_id": str(reply_id)})
            post_id = found["post_id"]
            if args.get("root_id") and str(args["root_id"]) != found["root_id"]:
                raise ValueError("root_id does not match exact comment")
            kwargs.update(reply_id=found["reply_id"], root_id=found["root_id"])
        else:
            post_id = post_id_arg(args)
            if args.get("root_id"):
                raise ValueError("root_id requires comment_id")
            if target:
                from xhh_operator.comments import locate_comment

                found = locate_comment(await operator.comments(post_id), target)
                kwargs.update(found)
            elif tool_name in ("heybox_reply_comment", "reply_comment"):
                raise ValueError("Reply requires comment_id or target_text")
        return await submit(operator.comment(post_id, content, **kwargs), "comment_id")
    if tool_name in ("heybox_search", "search_posts"):
        query = args.get("query") or args.get("keyword")
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Missing query")
        return await operator.search(query, kind="post", limit=bounded_int(args, "limit", 10))
    if tool_name in ("heybox_get_my_posts", "get_my_posts"):
        return await operator.posts(offset=0, limit=bounded_int(args, "limit", 10))
    if tool_name in ("heybox_get_user_info", "get_profile"):
        return await operator.profile()
    raise ValueError("Unknown HeyBox tool")


async def process_packet(packet):
    if load_config().get("enabled") is not True:
        return {
            "id": packet.get("id") if isinstance(packet, dict) else None,
            "success": False,
            "error_code": "module_disabled",
            "data": None,
        }
    write_started.set(False)
    req_id, writing, journaled = None, False, False
    try:
        if not isinstance(packet, dict):
            raise ValueError("RPC packet must be an object")
        req_id = packet.get("id")
        if not isinstance(req_id, (str, int)) or isinstance(req_id, bool) or not str(req_id):
            raise ValueError("Missing RPC id")
        tool = packet.get("tool")
        args = packet.get("args", {})
        if not isinstance(tool, str) or not isinstance(args, dict):
            raise ValueError("Invalid tool or args")
        writing = tool.lower().strip() in WRITE_TOOLS
        if writing:
            fingerprint = hashlib.sha256(
                json.dumps(
                    {"tool": tool.lower().strip(), "args": args},
                    sort_keys=True,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            previous = require_state().begin_request(str(req_id), fingerprint)
            if previous is not None:
                return previous
            journaled = True
        data = json_value(await handle_tool(tool, args))
        success = not isinstance(data, dict) or data.get("success", True) is True
        response = {
            "id": req_id,
            "success": success,
            "data": data,
            "error": None
            if success
            else data.get("error_code", data.get("status", "operation_failed")),
        }
    except Exception as exc:
        uncertain = isinstance(exc, WriteUncertain)
        # Unexpected failures can occur after a write (e.g. persistence/serialization).
        local = (
            isinstance(exc, (ValueError, OperatorError))
            and not uncertain
            and not write_started.get()
            and (not writing or journaled)
        )
        response = {
            "id": req_id,
            "success": False,
            "data": None,
            "error": "Write outcome unknown; do not resubmit"
            if writing and not local
            else "HeyBox request failed",
            "error_code": getattr(exc, "code", "invalid_request" if local else "internal"),
        }
        if writing:
            response["data"] = {
                "success": False,
                "accepted": False,
                "verified": False,
                "status": "failed" if local else "unknown",
                "submission_state": "not_submitted" if local else "unknown",
                "retry_safe": bool(local),
            }
    if journaled:
        require_state().finish_request(str(req_id), response)
    return response


def comment_items(thread):
    raw = (thread.get("result") or {}).get("comments") or thread.get("comments") or []
    for group in raw:
        if not isinstance(group, dict):
            continue
        items = group.get("comment") if isinstance(group.get("comment"), list) else [group]
        if not items:
            continue
        root = str(
            items[0].get("commentid") or items[0].get("comment_id") or items[0].get("id") or ""
        )
        for item in items:
            if isinstance(item, dict):
                yield root, item


def comment_timestamp(item):
    for key in ("create_at", "created_at", "create_time", "time"):
        try:
            value = float(item.get(key) or 0)
            if math.isfinite(value) and value > 0:
                return int(value / 1000 if value > 10**11 else value)
        except (ValueError, TypeError, OverflowError):
            continue
    return None


def comment_text(value):
    from xhh_operator.comments import plain_text

    if isinstance(value, list):
        value = " ".join(comment_text(v) for v in value)
    elif isinstance(value, dict):
        value = comment_text(value.get("text") or value.get("content") or "")
    return plain_text(str(value or ""))


async def flush_events(redis):
    # Publishing with zero listeners, failure, or a consumer crash does not acknowledge.
    for event in require_state().pending():
        await redis.publish(EVENT_CHANNEL, dumps(event))


PATROL_BLOCK_CODES = frozenset({"auth", "forbidden", "captcha", "rate_limit"})


def patrol_block_code(exc):
    code = getattr(exc, "code", None)
    return code if isinstance(exc, OperatorError) and code in PATROL_BLOCK_CODES else None


def patrol_delay(cfg, blocked_rounds=0):
    base = bounded_int(cfg, "poll_interval_seconds", 60, 86400)
    if blocked_rounds <= 0:
        return base
    # Do not amplify an account/edge denial across watched posts or retry every minute.
    return max(base, min(3600, 300 * 2 ** min(blocked_rounds - 1, 4)))


async def patrol_once(redis, cfg=None, now=None):
    cfg = load_config() if cfg is None else cfg
    now = int(time.time()) if now is None else now
    posts = cfg.get("monitored_posts", [])
    if not isinstance(posts, list):
        raise ValueError("monitored_posts must be an array")
    for post_id in posts:
        require_state().add_post(identifier(post_id))
    monitored_posts.update(require_state().posts())
    errors = []
    blocked_code = None
    # Refresh discovery every cycle, not just when no monitored posts exist.
    if operator and operator.session:
        try:
            result = (await operator.posts(offset=0, limit=20)).get("result") or {}
            for record in result.get("post_links") or []:
                post = record.get("link") or record
                post_id = post.get("linkid") or post.get("link_id")
                if post_id:
                    require_state().add_post(identifier(post_id))
                    monitored_posts.add(str(post_id))
        except Exception as exc:
            blocked_code = patrol_block_code(exc)
            errors.append({"stage": "discover", "code": blocked_code or type(exc).__name__})
    own_id = str((getattr(operator, "session", None) or {}).get("user_id") or "")
    ignored = cfg.get("ignored_user_ids", [])
    if not isinstance(ignored, list) or any(not isinstance(i, (str, int)) for i in ignored):
        raise ValueError("ignored_user_ids must be an array of stable ids")
    ignored_ids = {str(i) for i in ignored} | ({own_id} if own_id else set())
    for post_id in sorted(monitored_posts):
        if blocked_code:
            break
        try:
            thread = await operator.comments(post_id, allow_partial=True)
            if (thread.get("result") or {}).get("incomplete_reasons"):
                errors.append({"post_id": post_id, "code": "VISIBLE_COMMENT_COUNT_MISMATCH"})
            for root_id, item in comment_items(thread):
                comment_id = item.get("commentid") or item.get("comment_id") or item.get("id")
                if not comment_id or not root_id:
                    continue
                user = item.get("user") if isinstance(item.get("user"), dict) else {}
                author_id = str(
                    user.get("userid") or user.get("heybox_id") or item.get("userid") or ""
                )
                if author_id and author_id in ignored_ids:
                    continue
                timestamp = comment_timestamp(item)
                if timestamp is not None and now - timestamp > 30 * 86400:
                    continue
                payload = {
                    "event": "new_comment",
                    "event_id": f"heybox-comment:{post_id}:{comment_id}",
                    "post_id": post_id,
                    "comment_id": str(comment_id),
                    "root_id": root_id,
                    "author": user.get("username")
                    or user.get("nickname")
                    or item.get("author")
                    or "",
                    "author_id": author_id,
                    "content": comment_text(item.get("text") or item.get("content")),
                    "timestamp": timestamp if timestamp is not None else now,
                    "timestamp_known": timestamp is not None,
                }
                require_state().enqueue(payload)
        except Exception as exc:
            # Incomplete/transient reads never remove a post or advance a baseline.
            blocked_code = patrol_block_code(exc)
            errors.append({"post_id": post_id, "code": blocked_code or type(exc).__name__})
    await flush_events(redis)
    result = {"errors": errors, "pending": len(require_state().pending())}
    if blocked_code:
        result["blocked_code"] = blocked_code
    return result


async def comment_watcher_loop(redis):
    blocked_rounds = 0
    while True:
        try:
            cfg = load_config()
            if cfg.get("enabled") is not True:
                await asyncio.sleep(5)
                continue
            result = await patrol_once(redis, cfg)
            if result["errors"]:
                logger.warning("Comment patrol incomplete: %s", result["errors"])
            blocked_rounds = min(5, blocked_rounds + 1) if result.get("blocked_code") else 0
            interval = patrol_delay(cfg, blocked_rounds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Comment patrol failed; durable pending events retained")
            interval = 10
        await asyncio.sleep(interval)


async def init_services():
    global operator
    # Explicit runtime initialization only; no QR login or automatic browser launch.
    operator = Operator()


async def main():
    cfg = load_config()
    if cfg.get("enabled") is not True:
        logger.warning("HeyBox module disabled; no account or Redis connection opened")
        return
    global REQ_CHANNEL, RES_CHANNEL, EVENT_CHANNEL
    namespace = cfg.get("namespace", "corenyan")
    if (
        not isinstance(namespace, str)
        or not namespace.isascii()
        or not namespace.replace("_", "").isalnum()
    ):
        raise ValueError("Invalid namespace")
    REQ_CHANNEL, RES_CHANNEL, EVENT_CHANNEL = (
        namespace + ":heybox:" + suffix
        for suffix in ("tool:request", "tool:response", "event:comment")
    )
    import redis.asyncio as aioredis

    global state
    state = BridgeState(STATE_PATH, PROCESSED_COMMENTS_FILE)
    await init_services()
    async with operator:
        async with aioredis.from_url(redis_url(), decode_responses=True, protocol=2) as redis:
            async with redis.pubsub() as pubsub:
                await pubsub.subscribe(REQ_CHANNEL)
                watcher = asyncio.create_task(comment_watcher_loop(redis))
                try:
                    while True:
                        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                        if not msg:
                            await asyncio.sleep(0.05)
                            continue
                        try:
                            packet = json.loads(msg["data"])
                        except (ValueError, TypeError):
                            logger.warning("Ignored malformed RPC JSON")
                            continue
                        response = await process_packet(packet)
                        await redis.publish(RES_CHANNEL, dumps(response))
                finally:
                    watcher.cancel()
                    await asyncio.gather(watcher, return_exceptions=True)
                    state.close()


if __name__ == "__main__":
    (ROOT / "logs").mkdir(exist_ok=True)
    handler = TimedRotatingFileHandler(
        ROOT / "logs" / "heybox.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [HeyBox] %(levelname)s %(message)s",
        handlers=[handler, logging.StreamHandler()],
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
