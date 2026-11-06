# FreeCore HeyBox Redis bridge

The bridge is a deterministic adapter over `xhh_operator.Operator`; it does not use an LLM.
Importing it reads neither login sessions nor delivery state. Running it explicitly initializes the
existing Operator account; absent/invalid authentication fails operations, never starts login or a browser.

## Deployment and retained channels

`REDIS_URL` in the process environment takes precedence over this project's ignored
`config.json.redis_url`, then `config.json.redis.url` or a `redis` object with `host`, `port`,
`db`, optional `username`, `password`, and `ssl`. Missing configuration fails closed; there is no
silent unauthenticated fallback. The former source-embedded Redis value was migrated only to the
existing ignored project configuration; it must not be committed or copied into logs.

Channels are unchanged:

- Request: `freecore:heybox:tool:request`
- Response: `freecore:heybox:tool:response`
- Comment event: `freecore:heybox:event:comment`

Requests retain `{ "id": "stable-id", "tool": "heybox_get_comments", "args": {} }`.
Responses retain `{ "id": "stable-id", "success": true, "data": {}, "error": null }`.
Malformed JSON without a recoverable request identity is ignored; valid packets get structured errors.
The bridge converts dictionaries, dataclasses, Pydantic models, dates, paths, and tuples explicitly
and strips secret-named fields; unknown objects and nonfinite numbers fail instead of stringifying.

## Tools

| Tool | Arguments | Result / behavior |
|---|---|---|
| `heybox_publish_post` | Required `title`, `content`; optional string arrays `communities`, `tags` (or `hashtags`), `images`; boolean `draft=false` | Calls `Operator.write_post` once. No selection-error fallback, no default authored text, no implicit selection truncation. |
| `heybox_post_comment` | `post_id`, `content`, optional `images`; optional `comment_id`/`reply_id`, optional `root_id`; legacy `target_text`/`target_comment` alternative | Calls `Operator.comment`. Exact ID is located in a complete bounded thread and its root derived; supplied mismatching root fails before submission. When an ID exists, `target_comment` is approval-preview context and ignored for location; simultaneous `target_text` remains a conflicting locator and is rejected. Duplicate text is never chosen arbitrarily. |
| `heybox_reply_comment` | `content`, `comment_id`, optional `post_id`, `root_id`, `images`; legacy unique text also supported with `post_id` | Like comment, but an explicit reply target is required. |
| `heybox_find_comment` | `comment_id`, optional `post_id` | Returns `post_id`, `comment_id`, `reply_id`, `root_id`, `target`. Exact IDs may be found in explicitly incomplete visible threads; missing IDs never authorize a guessed target. Without a post, searches only configured/persisted monitored posts (at most 100), adds `scope=monitored_posts`, fails on ambiguity/read errors. Not a global comment search. |
| `heybox_get_post` | `link_id` / `post_id` / `postId` | Calls real `Operator.post`. |
| `heybox_get_comments` | Post ID; optional `limit=20` (page size 1–100), `max_pages=100` (1–100) | Bounded visible root + child thread. Upstream count mismatches/contradictory empty pages return `full_comments=false` with `incomplete_reasons`; other read failures remain errors. Never claims an incomplete read complete or permanently removes a post. |
| `heybox_search` | `query` or `keyword`, `limit=10` | Real `Operator.search(kind=post)`. |
| `heybox_get_my_posts` | `limit=10` | Real `Operator.posts`. |
| `heybox_get_user_info` | None | Real `Operator.profile`. |
| `heybox_pin_comment` | `post_id`, `comment_id`, boolean `pinned=true` | Explicit `supported=false`, `status=unsupported`, `submission_state=not_submitted`, no write. See capability boundary below. |
| `heybox_ack_comment_event` | `event_id` | Durable, idempotent acknowledgement of a known pending/delivered event. Unknown ID fails. |

Existing bridge aliases remain: `publish_post`, `publish_article`, `get_post`, `get_comments`,
`comment_post`, `search_posts`, `get_my_posts`, `get_profile`. The old root-comment sentinel is
accepted for compatibility, not generated as player-facing copy. New player-facing prose belongs
in the caller's lang package; the bridge emits machine status/identifiers, not recruitment text.

### Write outcome and retries

Successful writes require dictionary fields **`accepted is true`, `verified is true`, and a
nonempty `link_id` or `comment_id`**. Both envelope and data `success` are false otherwise.
No unconditional success message is emitted.

- `status=verified`, `submission_state=verified`: confirmed read-back, identified record.
- `status=unverified`, `submission_state=accepted`: server accepted, read-back not verified.
- `status=unknown`, `submission_state=unknown`: write outcome cannot be established.
- `status=failed`, `submission_state=not_submitted`, `retry_safe=true`: bridge preflight failed
  before entering the Operator write call (e.g. missing title, ambiguous comment, wrong root).
- `status=unsupported`: known unavailable operation; `retry_safe=false`.

`retry_safe=false` for every operation that entered a write, including exceptions that *might*
have happened during selection/upload. It is intentionally conservative. Unknown/accepted writes
must not be automatically retried with a new ID. Inspect the platform manually before deciding.

Write RPCs persist an intent journal before invocation. Resending the **same RPC ID and exact
arguments** returns the saved response without submitting again. Reusing an ID with different
arguments fails closed. A crash leaving an in-flight intent returns unknown on replay, not a
second submission. Responses also persist before Redis publication so publication failure can
be recovered with the same ID. This does not deduplicate unrelated IDs or cure legacy callers
that generate a fresh ID on each retry.

## Comment patrol and durable delivery

`bridge_state.sqlite3` is ignored runtime state. SQLite FULL synchronization persists monitored
posts, pending events, acknowledged IDs, and write RPC receipts. Do not delete this file to
"repair" delivery; corruption or persistence errors fail closed. Back it up as private application
data (it contains comment bodies). There is no arbitrary set truncation that reintroduces old IDs.

Each patrol uses bounded root/child pagination. The verified official frontend uses `page`/`is_first`
for root pages (no offset or invented `oldest` sort), and `root_comment_id`/`lastval` for children
(no link_id/limit override). 2026-10-28 live read-only validation showed the latter overrides can
produce misleading exhaustion. It also exposed upstream inflated child counts and empty pages still
advertising more floors. Patrol retains visible comments with `full_comments=false` and explicit
`incomplete_reasons`; this is not proof that hidden/deleted or otherwise unavailable comments were read.
SDK default reads and text matching remain strict; exact-ID matching may use a marked incomplete thread.
Other failures still fail closed without treating the thread as empty or complete.
It also refreshes the account's latest 20 posts each cycle; configured and previously discovered
posts persist. The discovery window is not an unlimited backfill of every account post.

There is no restart-wide baseline that discards comments created while offline. Explicit timestamps
older than 30 days are filtered; `create_at`, `created_at`, `create_time`, and `time` accept seconds
or milliseconds. Missing/invalid time is delivered using observation time and
`timestamp_known=false`. Identity filtering uses the account's stable `user_id` and optional
`ignored_user_ids`, never nickname substrings, group numbers, or branding in comment content.
Comments without a stable author ID are delivered rather than guessed to be the bot.

Events retain the legacy fields and add reliable targeting/deduplication:

```json
{
  "event": "new_comment",
  "event_id": "heybox-comment:900:11",
  "post_id": "900",
  "comment_id": "11",
  "root_id": "10",
  "author": "synthetic author",
  "author_id": "7",
  "content": "synthetic comment",
  "timestamp": 2000000000,
  "timestamp_known": false
}
```

The event is saved **before** publication and remains pending after publication, even when Redis
reports subscribers. Every patrol republishes pending events until acknowledged: **at-least-once,
not exactly-once**. The consumer must atomically persist a work item keyed by `event_id`, then call
`heybox_ack_comment_event`. If a consumer crashes after persistence but before ACK, deduplicate
on replay and ACK again. Old consumers can still read the unchanged channel/fields, but without
ACK they receive repeated events; deploy the ACK-capable consumer before enabling this watcher.

Old `processed_comments.json` list IDs are imported once and the original file retained. Those
IDs were previously treated as processed without proof of delivery: the bridge cannot infer
which represent genuinely completed work and cannot recover comments already suppressed by
that historical file. New events use compound post/comment IDs. A genuinely first deployment
may send recent or undated backlog; the consumer should use stable event IDs and review policy.

## Pin capability / verification boundary

The existing Operator, CLI, SDK, docs, and browser helpers expose **no evidenced comment-pin
write method**. Official public post JavaScript was inspected anonymously: the current documented
[post module](https://static.max-c.com/static/heybox/webapp/@heybox-spa/website/app/index-BRmpZOWP.js)
contains a `top_comment` display class and `is_link_owner` presentation, comment subthread reads,
and comment support/like. Those display fields are not evidence of an authorized pin write API
or a safe UI action. The repository's parser similarly only reads `is_top`.

Consequently group-number comments can be posted/replied to and targeted precisely, but **automatic
pinning is not implemented/verified**. The tool explicitly returns unsupported rather than guessing
an endpoint, injecting an unverified browser action, or claiming that posting alone pinned anything.
This remains an acceptance blocker when automatic pinning is a required feature. A future change
needs an actual platform contract (endpoint + fields or authenticated UI affordance), account/post
ownership and exact target checks, one submission, and read-back of that same comment's pin state.

## Deployment validation (2026-10-28)

The user explicitly approved restarting only the existing managed HeyBox instance through MCSManager;
no second bridge or browser was started. Read-only RPC profile and owned-post discovery succeeded using
the existing account; real thread reads identified and corrected the pagination parameter incompatibility.
One newer owned thread contained 1 visible comment and completed. An older thread returned 216 visible
root groups / 379 visible items, but has contradictory upstream totals and root exhaustion: it remains
explicitly incomplete rather than being labeled complete. No comment bodies or account secrets were
printed during diagnosis.

Final preserved adapter regression suite: 207 passed / 19 skipped / 1 deselected (the pre-existing
published-doc branding check rejects an upstream attribution in README; provenance was preserved).
The reduced count versus earlier temporary bridge tests reflects their requested deletion, not missing
preserved tests. Temporary verification ran via stdin with bytecode/cache generation disabled.
The final managed bridge is RUNNING(3), started=6 (root PID 3332). Deployed RPC reads succeeded:
new thread 1 visible group / full_comments=true; older thread 216 visible groups / full_comments=false;
exact comment 859193841 resolved to root 859166106 in that older thread. Those identifiers are
read-only target evidence, not a publication authorization. Core restarted through the same daemon
for comment grounding (started=7), QQ remains started=2.

## Follow-up verification (2026-10-29)

The one-submission comment receipt check now scans bounded root/child pages rather than only the first
page. Child reads start at the approved reply target and advance by returned lastval / last visible ID;
root reads use page/is_first/time_desc. Repeated cursors/root pages and max_pages stop the read, never
trigger another submission. Matching requires the exact receipt ID, author, body, reply target and
submitted media; hidden-count mismatches do not falsely turn a matching receipt into a failed write.
Synthetic stdin validation covered 24 scenarios + 5 invalid page bounds, including wrong target/author,
second/third pages, cursor fallback, duplicate cursor, and no returned ID. The preserved adapter suite
still passes 207 / 19 skipped / 1 deselected. No test script or image artifact was created.

Approval text and the QQ image now include root target, sections/tags/media and draft mode. The image
also renders the full follow-up authorization, wraps long original comments/titles, uses exact ticket
IDs in command examples, and warns explicitly on its bounded 500-line preview rather than silently
clipping. Missing font exports previously produced undefined SVG fonts; they now use configured/fallback
fonts. Real sharp PNG rendering and mocked SVG field assertions passed entirely in memory. No image was
sent to a real QQ user for testing.

All three already-approved instances were updated only through MCSManager. Verified RUNNING(3):
Core started=8 / root PID 9328; QQ started=3 / root PID 5168; bridge started=7 / root PID 9364.

Draft isolation was also deployed in the Core workflow: tickets carrying `draft=true` disable
follow-up authorization, never add a delayed recruitment job, and cancel any legacy persisted draft
job before it can call the bridge. Formal posts retain the bounded 120–180 second follow-up policy.
The workflow reports unsupported pin receipts as not submitted, rather than treating them as ordinary
failed writes or retrying them. After this Core-only update, verified RUNNING(3): Core started=9 /
root PID 1628; QQ remains started=3 and bridge remains started=7. Fifteen documented command-route
checks plus private-owner-only approval and read-only tool allowlist checks passed using only mocks.

Anonymous official frontend evidence remains negative for a pin contract: the comment menu contains
only report, `top_comment` is a style/read field, and `/bbs/app/comment/support` is like/unlike, not
pin. This checks the supplied public hash and direct dependencies only; it does not prove that an
undocumented app/internal endpoint cannot exist.

### Latest health supersedes earlier read success

A later read-only post call was classified by the old code as `AuthError`; the managed bridge's
profile RPC also returned `success=false,error_code=auth`. That classification was too broad: the old
HTTP layer mapped both 401 and 403 to auth. Subsequent anonymous checks found HTML HTTP 403 from the
website root, API root and signed `/account/restore_login`, without any session. Static official JS
returned HTTP 200. These are access-denial observations, not proof of an expired saved account.
Earlier successful account/post/comment reads are historical evidence, not current reachability.
No automatic login/browser, credential refresh, routing change or write retry was attempted.

Core command handling now preserves the bridge error-code classification in profile/status/get/comments
and reply-target lookup. Auth, rate limit, network and protocol errors produce language-package messages;
a failed lookup is not mislabeled a nonexistent target, and status no longer hides failure inside a
nonempty data envelope. Mock assertions confirm zero draft/ticket creation on target-lookup auth failure.
Core-only deployment verified RUNNING(3), started=10 / root PID 9612; QQ and bridge unchanged. Existing
post-readback regressions passed 5/5 (26 deselected), including stale body and delayed title checks.

Follow-up synthetic auth safety checks used fake APIs and SQLite `:memory:` only: failed initial account
verification sent zero post/comment forms; patrol auth errors retained monitored posts and pending
outbox events, re-delivering without ACK. An unknown-write/auth receipt retained the original ticket
and prevented reapproval, retry and follow-up creation. No synthetic script or session file was created.

The existing CLI supports explicit `login` QR and `login --browser` modes; neither is invoked by the
bridge. Operator loads the session once in its constructor. After the account holder explicitly
completes login in the same runtime environment/session location, the existing bridge must be reloaded
through its approved MCSManager instance so it stops using the old in-memory cookies. Do not start a
second bridge, remove the old session on failure, or paste cookies into chat.

### Anonymous 403 diagnosis and corrected classification

Anonymous website/API root responses: HTTP 403, openresty/1.21.4.3, text/html, title `403 Forbidden`,
159 bytes. Anonymous signed account restore: HTTP 403 HTML, 561 bytes, no JSON status. Static official
JS: HTTP 200. No proxy environment variables were set, and WinHTTP reports direct access. This does
not identify the denial's exact cause (platform edge, network route or another intermediary); do not
claim a confirmed IP ban, session expiration, or universal platform outage.

The supplied public `index-DTmiiO5L.js` explicitly maps `heybox_app.prod` to `https://api.xiaoheihe.cn`;
its GET error wrapper preserves HTTP status and marks HTTP errors as `network_error`. No migration
proof was found. Because the homepage is denied, its current referenced hash cannot be confirmed.

HTTP 403 now raises `OperatorError(code='forbidden')`, separately from HTTP401/business login status.
Core has distinct forbidden/captcha language, and legacy REST maps forbidden to403. Existing tests
were updated rather than adding disposable scripts. Adapter offline suite: 210 passed,19 skipped,
1 known product-doc test deselected,2 deprecation warnings. Inline Core and HTTP mocks confirm
separate 403/auth/captcha classifications, no retry, no body/credential echo and no draft creation.

Deployment preflight rejected changed runtime state before sending any restart: HeyBox is now
STOPPED(0),started=9,pid=0 (previously RUNNING,started=7,pid9364). Core remains RUNNING,started=10,
pid9612; QQ RUNNING,started=3,pid5168. Do not override a potentially user-stopped bridge. The latest
403 changes were initially held on disk until the user explicitly approved restoring the original
bridge and loading the fix. After that approval, the exact bridge was opened through MCSManager and
the exact Core restarted: both RUNNING(3), bridge started=10/pid680, Core started=11/pid10076;
QQ unchanged started=3/pid5168. One profile RPC now returns `success=false,error_code=forbidden`,
proving classification is loaded while the actual platform denial remains unresolved. No QR/login,
network changes, publishing or pinning was attempted.

### Patrol denial fan-out and cooldown

Discovery or a comment read that raises `auth`, `forbidden`, `captcha` or `rate_limit` now stops
further platform reads in that patrol cycle. Persisted posts/outbox events remain intact, and queued
events are still delivered to Redis without implicit ACK. Non-global/per-post protocol errors do not
silently suppress reads of unrelated posts. Errors retain a safe category without response bodies.

Consecutive blocked cycles wait at least 300,600,1200,2400,3600 seconds (cap1h), or a longer configured
poll interval. A non-blocked cycle resets to the configured interval. This changes background reads
only: no write retries, automatic login or queued-event deletion is introduced. The bounded cooldown
lives in the watcher process and resets at process restart; manual RPC reads remain available.

Inline SQLite-memory tests covered eight blocked discovery/comment combinations, healthy/per-post
continuation, pending event delivery and retention, delay bounds, recovery reset and cancellation.
Preserved adapter suite remains 210 passed/19 skipped/1 known doc-test deselected. Bridge-only managed
update verified RUNNING(3),started=11/pid8932; Core started11/pid10076 and QQ started3/pid5168 unchanged.
The real startup patrol log reports only `discover/forbidden`, not one failure per monitored post.
These prove runtime loading and request containment, not restored platform access.

Closing passive checks: no xiaoheihe/max-c/heybox override matched the Windows hosts file. Default
resolver returned www.xiaoheihe.cn A101.126.6.43 and api.xiaoheihe.cn CNAMEapi.slb.xiaoheihe.cn,
A101.126.44.167. Resolution succeeds; these records are not proof against DNS/network faults. One
anonymous website-home GET still returned403/openresty HTML159bytes. Do not change hosts, force IPs,
rotate proxies or delete sessions based only on this evidence. Same-host ordinary-browser comparison
or an operator/platform resolution is still needed before live account/publication acceptance.

No new login or real publish/reply/pin write was performed in these validation steps. Unit tests and
historically successful reads do not demonstrate real publishing or pin permissions.
