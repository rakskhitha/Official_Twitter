# X Daily Monitor

Every day this job does two things for the accounts listed in the database table `monitor_targets` (or in `config/username.json`, see "Where the account list comes from" below):
- It collects every **new post**, including replies and retweets.
- It collects **profile info**, refreshed weekly.

Each run writes to these outputs (each can be switched on or off in `config/input.json`):
- **SQLite:** `data/x_monitor.db`. Always needed: it's where collection resumes from, and the source for the pipeline database.
- **Pipeline database:** the shared database's existing `twitter_posts`, `twitter_profiles` and `twitter_pipeline_runs` tables (see section 5), set with `DB_*` in `.env`.
- **JSON:** `data/json/profile.json` (all users' info) and `data/json/tweets.jsonl` (all tweets, one per line, appended on every run).
- **PostgreSQL `x_monitor`:** the database set in `POSTGRES_DSN`. Switched off now that the pipeline database is used.

`plan.md` has the full design and the API details.

## 1. Setup (once)

```powershell
cd D:\Official_Twitter
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
copy .env.example .env      # then edit .env
```

### `.env` settings
- `X_BEARER_TOKEN`: the Bearer Token for your app at https://console.x.com. Buy some credits and **set a spending limit** there as well.
- `POSTGRES_DSN`: the PostgreSQL connection string, for example `postgresql://postgres:YOURPASSWORD@localhost:5432/x_monitor`. Only needed when the `postgres` output is on.
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`: the **pipeline database**. To switch between a local test copy and the real one, comment one set of lines out with `#` and remove the `#` from the other. Each run writes to `run.log` which database it used, e.g. `Pipeline DB: connected to shaktidb_social on 10.10.116.215:13366` (never the password).

### Create the PostgreSQL database
Create it once, either in pgAdmin or with:
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -c "CREATE DATABASE x_monitor"
```
The tables are created automatically on the first run.

### Configuration (`config/` folder)
| File | What goes in it |
|---|---|
| `config/username.json` | The handles, each with an on/off switch (used when `usernames_source` is `"file"`, and as the last fallback) |
| `config/input.json` | All other options (see below) |

### Where the account list comes from (`usernames_source` in `input.json`)
**`"database"` (current setting):** the accounts are the rows of `monitor_targets` (in the `DB_*` database) with `platform = 'twitter'` and `target_type = 'username'`, any `group_name`, in `sort_order` order. Manage them there, for example in pgAdmin:
- **Add an account:** insert a row (`platform` `twitter`, `target_type` `username`, `value` = the handle, `enabled` true). It's collected from the next run on, with no restart.
- **Stop an account:** set `enabled` to false. Its stored data is kept and it's marked inactive.
- **Cleaned like `username.json`:** the leading `@` is optional, invalid names and duplicates (ignoring letter case) are skipped with a warning (`monitor_targets id 5: …`). The table's unique rule doesn't catch duplicates when `group_name` is empty, so the program does.
- **The X user id:** after an account is first collected, the program writes its X user id into `external_id`, only where that's empty. Nothing else in the table is ever changed.

If the table can't be read at the start of a run:

| Order | The run uses | Logged as |
|---|---|---|
| 1 | the table | (normal) |
| 2 | the list saved at the last successful read (kept in SQLite, per database) | warning |
| 3 | `config/username.json`, only if no list has been saved yet | warning |
| 4 | nothing: the run stops | error |

**Copying `username.json` into the table** (one time, per database):
```powershell
.venv\Scripts\python -m src.import_usernames            # dry run: shows what is in both / only in one, writes nothing
.venv\Scripts\python -m src.import_usernames --apply    # adds the file's accounts the table doesn't have yet
```
A name already in the table (ignoring letter case and `@`) is never added again, and existing rows are never changed. New rows follow the existing Twitter rows: `group_name` and `url` empty, `extra` = `{"src_id": <id in username.json>}`, `sort_order` after the table's highest.

**`"file"`:** the accounts come from `config/username.json` as before.

`config/username.json`:
```json
[
  { "id": 1, "username": "BBCWorld",    "enabled": true },
  { "id": 2, "username": "dgprPaknavy", "enabled": false }
]
```

Each entry has three fields:
- **`id`:** just a row number for you. The code doesn't use it, and it is **not** the X user ID.
- **`username`:** the @handle. The leading `@` is optional, and case doesn't matter.
- **`enabled`:**
  - `true`: monitored.
  - `false`: skipped. If it was monitored before, its stored data is kept and it is marked inactive.
  - Missing: counts as `true`.

**Problems that don't stop the run:** a username that isn't valid on X (allowed are letters, digits and `_`, at most 15 characters) is skipped. So is a handle listed twice. Each one is logged as a warning on every run, so the rest of the list still runs.

**Problems that do stop the run:** the run stops with an error if no entry is enabled, or if `enabled` is anything other than `true` or `false`.

**Renamed accounts:** the first time a row is looked up, it's linked to that account's permanent X user ID, and it stays linked to that ID.
- **The account renames:** it keeps being collected under its new handle, with status `renamed`. You don't need to edit `username.json`.
- **Someone else takes the old handle:** the new holder is **never** followed. A warning in the log names both user IDs.
- **To follow the new holder instead:** this isn't possible through `username.json` yet. The link stays even if you remove the row and add it back. It needs the optional `x_user_id` pinning (code_review.md, B3), or the old link must be removed from the stored data.
- **The same account listed twice** (for example the old and the new handle): it's collected once, and a warning is logged.

`config/input.json`:
| Option | Default | Meaning |
|---|---|---|
| `usernames_source` | `"file"` | `"database"`: accounts from `monitor_targets`; `"file"`: from `username.json` (see above) |
| `lookback_days_first_run` | `2` | How far back the first run for a new handle goes |
| `profile_refresh_days` | `7` | How often profiles are refreshed |
| `max_pages_per_user_per_run` | `10` | Safety cap: at most 10 × 100 = 1,000 posts per handle per run |
| `max_consecutive_failures` | `10` | Stop the run when this many accounts **in a row** fail with server/network errors or 403s (an X outage, connection or app-access problem) |
| `run_time_limit_minutes` | `50` | A run **stops cleanly** after this long. Finished accounts are saved, and the rest go first in the next run. |
| `scheduler_kill_after_minutes` | `55` | The scheduler **kills** a run that is still going after this long (a stuck run) |
| `schedule_interval_minutes` | `60` | How often the scheduler starts a run (section 4). The three time settings must satisfy run limit < kill time < interval. |
| `schedule_run_on_start` | `true` | Whether the scheduler runs once immediately when it starts |
| `outputs` | `sqlite`, `postgres`, `json` on; `pipeline` off | Turns `sqlite`, `postgres`, `json` and `pipeline` on or off. PostgreSQL can't be the only one enabled, and `pipeline` needs `sqlite`. |
| `sqlite_path` | `data/x_monitor.db` | Where the SQLite file goes |
| `json_dir` | `data/json` | Folder for `profile.json` and `tweets.jsonl` |
| `log_dir` | `logs` | Folder for `run.log` and `scheduler.log` |
| `log_max_mb` | `50` | `run.log` is rolled over to `run.log.1` when it reaches this size (checked at the start of each run) |
| `log_backups` | `5` | How many old run logs are kept (`run.log.1` … `run.log.5`) |
| `scheduler_log_max_mb` | `10` | Same for `scheduler.log` |
| `scheduler_log_backups` | `5` | Same for `scheduler.log` |
| `low_credit_warning_usd` | `2.0` | Log a warning when your X credit balance is below this many dollars (`0` = never) |
| `api` | (built in) | Optional overrides for the field lists; see section 2 |

To use a different config folder, pass `--config <folder>`.

## 2. Verify before the first real run

```powershell
.venv\Scripts\python -m src.run_daily --check     # checks config, .env and outputs; no API calls, no cost
.venv\Scripts\python -m src.probe XDevelopers      # 2 real API calls (about $0.035); saves raw responses to data\probe\
```

**What `src.probe` shows:**
- whether the token works
- which field names the live API returns (`note_tweet` or `note_post`, and so on; see plan.md section 1.5)

If the API rejects a field name with a 400 error, override the field lists in `config/input.json`:
```json
"api": { "tweet_fields": ["created_at", "..."], "user_fields": ["..."] }
```

## 3. Run

```powershell
.venv\Scripts\python -m src.run_daily                          # one run now (the scheduler does this hourly)
.venv\Scripts\python -m src.run_daily --force-profile-refresh  # refresh all profiles now
run_daily.bat                                                   # same as the first line
```

### What each run does
1. **First run per user:** fetches the last 2 days of posts. After that, each run fetches only posts newer than the last one stored.
2. **Profiles:** looked up when a username is new, and otherwise every 7 days.
3. **Writing:** every output is written. A user's cursor in an output only moves forward once that output has saved the posts.
4. **When an output missed a save** (it was down, or a save failed): at the start of the next run, the JSON output and the `x_monitor` PostgreSQL are **filled in from SQLite**, locally and at no API cost, so nothing is fetched or paid twice. Only when SQLite itself missed a save (it holds the only complete copy), or the outputs differ in a way that can't be filled in exactly, does the run fetch those posts from X again (it starts from the oldest position across the outputs, logged as a warning).
5. **More new tweets than the page cap** (`max_pages_per_user_per_run`, 10 pages = 1,000 tweets): this happens for example after the PC was off for days. The newest tweets are saved, and the range that wasn't fetched is recorded as a **gap**, together with the stored position.
   - **Filling the gap:** on the following runs, each account's leftover page budget is used to fetch exactly that range (X's `since_id` + `until_id`) until the gap is filled. New tweets are always fetched first.
   - **If X can no longer return the tweets** (X only serves an account's latest 3,200), the gap is closed and reported as an error in the log, so a loss is never silent.
   - **Where gaps are stored:** in `monitored_users.fetch_gaps` (SQLite) and `data\json\_state.json` (`"gaps"`).
6. **Logs:** every run appends to `logs\run.log`, and a summary with the estimated cost goes to the `runs` table and `data\json\_runs.jsonl`.

### Credits used per run (real, from X)
X doesn't report what each request costs, but it does report your **credit balance** (`GET /2/usage/credits`). Every run reads it **at the start and at the end**, and the difference is what X actually charged for that run:
```
INFO  [20261007T063011Z-eb9f57] x_monitor: Credit balance at start: $12.400 (prepaid $10.400, free $2.000).
INFO  [20261007T063011Z-eb9f57] x_monitor: Credit balance at end: $12.155 (prepaid $10.155, free $2.000).
INFO  [20261007T063011Z-eb9f57] x_monitor: Run … finished: … posts=40, referenced tweets=9, user reads=0, est. cost $0.245, credits used $0.245, balance $12.155, errors=0
```
- **`est. cost`** is the program's own estimate (items returned × price). **`credits used`** is the real amount from X.
- **The real amount can be lower than the estimate:** X doesn't charge the same tweet or profile twice in one UTC day.
- **Where it's stored:** both values are saved per run in the `runs` table (`credits_before`, `credits_after`, `credits_used`) and in `data\json\_runs.jsonl`.
- **When it's "not measurable":** if the balance **went up** during the run (a top-up, auto-recharge or new free grant), `credits used` is shown as not measurable and stored as empty.
- **When it's "unknown":** if the balance can't be read (for example your plan doesn't allow that endpoint), the run carries on normally, logs a warning, and records `credits used` as unknown. The balance check makes **one** attempt, never waits, and isn't blocked by the run's time limit, so it can't slow down or stop a run.
- **Low-balance warning:** when the balance at the end is below `low_credit_warning_usd` (default $2), a warning reminds you to top up before runs start failing with `credits depleted`.
- **A possible delay:** X doesn't document how quickly the balance reflects a charge. If it lags, part of one run's cost can appear in the next run's figure, but the totals over several runs are still correct.

### Exit codes
| Code | Meaning |
|---|---|
| 0 | OK. There may be warnings: problems the program handled itself (see below) |
| 1 | Finished, but something failed: some accounts or outputs had errors (see the log) |
| 2 | Stopped: bad config, missing token (401), app-level access denied (403), out of credits, or an outage (10 accounts in a row failed with server/network errors or 403s) |

**Errors and warnings:** every problem in a run is logged once, and saved with the run summary as `{time, account, message}` (SQLite `runs.errors_json` / `warnings_json`, `_runs.jsonl`, and the dashboard table `twitter_pipeline_runs`).
- **Error:** something failed, so data wasn't collected or saved this run (it's retried next run), or the run was stopped. Examples: an account's posts couldn't be fetched, an output was unavailable, saving failed, the profile lookup failed, out of credits.
- **Warning:** something unusual that the program handled itself, with no data lost. Examples: an account in `username.json` doesn't exist or is suspended, a rename, a handle now used by another account, a page-cap gap, a gap X can't return, the time limit reached, a low credit balance, a rate-limit wait, invalid `username.json` entries.
- **Run status:** `success` (no errors, warnings allowed), `partial_success` (some errors, the rest collected) or `failed` (the run was stopped).

**When one account fails:**
- **Only that account is affected.** For example, X keeps returning a server error for it, or it's not found. It's logged, its stored position doesn't move, and it's retried next run. All other accounts are still collected.
- **Errors that affect every account stop the run immediately:**
  - a bad token (**401**)
  - a **403 that X labels as app-level** (`client-forbidden`, `unsupported-authentication` or `usage-capped`, meaning the app, project or plan can't use the endpoint)
  - being out of credits or over the usage cap
  - a rate limit that never clears
- **Any other 403** (for example `not-authorized-for-resource`, or no error type) fails **only that account**, which is retried next run.
- **Many accounts in a row failing** with server or network errors, or with 403s (`max_consecutive_failures`, default 10), almost certainly means X or your connection is down, or your app has lost access. The run then stops with a clear message, and the accounts it didn't reach are collected next run.
- **Any answer from X resets that count,** including a definite "not found". Answers like that don't count toward it.

## 4. Schedule hourly (APScheduler)

```powershell
.venv\Scripts\python -m src.scheduler      # or double-click run_scheduler.bat
```

### What the scheduler does
- **Timing:** it starts a run **immediately**, then **every 60 minutes**. It keeps going until you press Ctrl+C or close the window.
- **Each run is a separate process** (`python -m src.run_daily`). That means:
  - changes to the account list (`monitor_targets` or `config/username.json`) and to the run options in `config/input.json` take effect on the next run, without a restart
  - a crash in one run can't stop the scheduler
- **Settings** in `config/input.json`. These two are only read when the scheduler starts, so restart it after changing them.
  - `schedule_interval_minutes` (default `60`)
  - `schedule_run_on_start` (default `true`)

### Safety rules
- **No overlap:** if a run is still going when the next one is due, the due run is **skipped**, not started twice. The run also holds an operating-system file lock as a second guard.
- **Every run ends before the next one is due.** A normal run takes only a few minutes; these limits only catch problems:
  1. **At 50 minutes** (`run_time_limit_minutes`), the run stops cleanly. It finishes nothing new after that time, and a rate-limit wait that would go past that time isn't started.
     - **Nothing is lost:** every finished account is saved.
     - **Who goes next:** accounts it didn't reach go **first** in the next run. The order is "least recently attempted first", so accounts that keep failing don't pile up at the front.
     - It's logged as `Time limit reached … after N of M accounts`, with exit code 1.
  2. **At 55 minutes** (`scheduler_kill_after_minutes`), the scheduler **kills** a run that is stuck in a way it can't notice itself, and logs `Run killed after 55 min` in `scheduler.log`.
     - **This is safe:** the lock is released automatically, the databases roll back unfinished writes, and the JSON output removes half-written lines on the next start.
  3. **Database limits:** PostgreSQL statements are limited to 60 seconds, with keepalives to detect a dead connection. SQLite waits at most 30 seconds for a lock. X requests have a 10-second connect timeout and a 30-second timeout per read.
- **Missed runs:** if the PC was asleep and several runs were missed, it makes **one** catch-up run when it wakes. Missed time loses no posts, because each run continues from the last stored post. The only exception is a user posting more than 3,200 posts in between.
- **Crashed runs:** if a run crashes or is killed, the lock is released automatically, so the next run still starts.

### Logs
There is **one current file for each**. **Every line of `run.log` carries the run ID** in square brackets, after the date, time and level:
```
2026-10-07 11:37:36,854 INFO    [20261007T060736Z-30d73f] x_monitor: @BBCWorld: 3 new post(s) since 18800….
```
- **What the ID means:** `20261007T060736Z` is the run's start time in UTC, and `-30d73f` is a random suffix.
- **Where it's used:** the scheduler chooses the ID and passes it to the run, so the **same ID** appears in `scheduler.log` (`Run 20261007T060736Z-30d73f starting / ended …`), on every line in `run.log`, and in the run summary (the `runs` table and `data\json\_runs.jsonl`).
- **Runs started by hand** create their own ID. Lines from `--check` show `[check]`.
- **To see one run's lines:** `Select-String -Path logs\run.log* -Pattern "20261007T060736Z-30d73f"`.

| File | Contents | Size limit |
|---|---|---|
| `logs\run.log` | What each run did. **Every run is appended** to this file. | At **50 MB** it becomes `run.log.1` and a new `run.log` starts. Older copies shift to `.2` … `.5`, and the oldest beyond 5 is deleted. The check happens only **at the start of a run**, so a run's lines are never split across two files. |
| `logs\scheduler.log` | When each run started and ended, with its exit code | At **10 MB** it becomes `scheduler.log.1`, the same way. 5 old copies are kept. |

- **`.1` is always the most recent old copy.** At hourly runs, `run.log` rolls over about every 4–5 months, and `scheduler.log` about every 2 years. Disk use is capped at about 300 MB and 60 MB.
- **To search everything,** including the old copies: `Select-String -Path logs\run.log* -Pattern "BBCWorld"`.
- **If a log file is locked** by another program when a rollover is due, the rollover waits until the next run, and logging carries on in the current file.
- **The size limits and copy counts** can be changed in `config/input.json` (see the option table above).
- **Run summaries are kept permanently,** independent of log rotation: the `runs` table and `data\json\_runs.jsonl` keep one summary per run.

### Cost of hourly runs
Each post is still fetched and paid for only once, and profiles are still refreshed weekly. Running every hour mostly means more requests that return 0 new posts, and those cost nothing. That's about 143 requests per run, far below X's limit of 10,000 per 15 minutes.

### Keeping it running on this PC
The scheduler only runs while its window is open. To start it automatically each time you log in to Windows:
```powershell
schtasks /Create /TN "X Monitor Scheduler" /TR "D:\Official_Twitter\run_scheduler.bat" /SC ONLOGON
```
**Use only one way of scheduling.** Don't also add a separate Task Scheduler entry for `run_daily.bat`. If one exists, it can't cause duplicate runs because of the lock, but it would be redundant.

**Later, on a Linux server:**
1. Copy the project, `.env`, the `config/` folder and the `data/` folder.
2. Run `.venv/bin/python -m src.scheduler` as a service, for example with systemd and `Restart=always`.

## 5. Outputs

### Pipeline database: `twitter_posts`, `twitter_profiles`, `twitter_pipeline_runs` (`src/storage/pipeline.py`)
These tables already exist and are read by downstream processes. The program only writes rows to them, in the formats the previous pipeline used, and creates nothing. At the end of each run it copies from SQLite whatever the database doesn't have yet. If the database was unreachable, the next run delivers everything missed, at no API cost. SQLite records what was delivered to each database (`pipeline_*` tables), so a test copy and the real database each start from their own switch-on.

| Table | What is written |
|---|---|
| `twitter_posts` | Every post **first collected after switch-on** (posts collected earlier aren't loaded). Insert only: a `tweet_id` already in the table is skipped, never changed. `keyword` = the account's name in `username.json`; `content_type` = `tweet` / `retweet` / `quote` / `reply`; `text` = the full text, and for a retweet `RT @author: ` + the complete original; `attachments` = `[{url, type}]` (videos as their mp4 file); hashtags and mentions without `#` / `@`; `parent_tweet_id` for replies. Also `referenced_tweet_id`, `referenced_full_text`, `referenced_author_id`, `conversation_id` and `raw_json`. **Not written** (downstream fills them): `embedding`, `text_search`, `entities`, `translated_text`, `detected_language` |
| `twitter_profiles` | All profiles at switch-on, then a row again whenever its content changes (the table holds each account's latest state). Matched by `user_id`, else by `username` ignoring letter case. A rename updates the same row. Also `user_id`, `profile_image_url`, `status` (`ok` / `renamed` / `protected` / `suspended` / `not_found`, and a suspended account keeps its last profile). `join_date` in X's text format, e.g. `Fri Jun 28 20:09:40 +0000 2013`. **`last_scraped_at`**: when the account's posts were last collected and saved; **`posts_last_run`**: how many of its own posts that run saved (new + recovered from gaps, not referenced tweets; `0` = checked, nothing new). If an account can't be collected in a run, both keep their previous values. Because they change every run, each collected account's row is updated every run (`ingested_at` = last written; `last_updated` = last profile refresh from X) |
| `twitter_pipeline_runs` | One row per run since switch-on, written at the end of the run: `pipeline_name` = `twitter`, `status`, accounts loaded and processed, posts collected, rows inserted and skipped, the numbers of warnings and errors, `warning_messages` and `error_messages` (the full lists), `credits_used` and `credits_balance` |

The columns this program adds to those tables:
```sql
ALTER TABLE twitter_posts
    ADD COLUMN IF NOT EXISTS referenced_tweet_id text, ADD COLUMN IF NOT EXISTS referenced_full_text text,
    ADD COLUMN IF NOT EXISTS referenced_author_id text, ADD COLUMN IF NOT EXISTS conversation_id text,
    ADD COLUMN IF NOT EXISTS raw_json jsonb;
ALTER TABLE twitter_profiles
    ADD COLUMN IF NOT EXISTS user_id text, ADD COLUMN IF NOT EXISTS profile_image_url text,
    ADD COLUMN IF NOT EXISTS status text, ADD COLUMN IF NOT EXISTS last_scraped_at timestamptz,
    ADD COLUMN IF NOT EXISTS posts_last_run integer;
ALTER TABLE twitter_profiles ADD CONSTRAINT twitter_profiles_user_id_key UNIQUE (user_id);
ALTER TABLE twitter_pipeline_runs
    ADD COLUMN IF NOT EXISTS error_messages jsonb, ADD COLUMN IF NOT EXISTS warning_messages jsonb,
    ADD COLUMN IF NOT EXISTS credits_used numeric, ADD COLUMN IF NOT EXISTS credits_balance numeric;
```
`--check` connects to the database and reports any of these columns that are missing. A run then delivers nothing (an error) until they exist, and afterwards delivers everything at once.

### SQLite: 5 tables (`src/storage/schema.sql`)
| Table | Contents |
|---|---|
| `monitored_users` | `user_id`, usernames, status (`ok` / `renamed` / `protected` / `not_found` / `suspended`), cursor |
| `user_profiles` | **One row per account, always the latest profile** (bio, counts, verification, status, raw JSON), plus a `changes` list of every change to a tracked field (see below) |
| `posts` | One row per post: `post_url` (`https://x.com/<username>/status/<post_id>`), text, `full_text` (the long-form text), `post_type` (`original` / `reply` / `quote` / `retweet`), `referenced_post_id`, the referenced tweet's `referenced_full_text`, `referenced_author_id` and `referenced_created_at` (see the notes below), metrics at fetch time, raw JSON |
| `media` | Photos, videos and GIFs attached to posts |
| `runs` | One row per run: `status`, counts (`posts_new` = the accounts' own posts, `referenced_reads` = referenced tweets, `post_reads` = both, accounts loaded and processed, rows added to the pipeline database), estimated and real cost, errors and warnings |

Plus 4 small tables, `pipeline_targets` and `pipeline_sent_posts` / `_profiles` / `_runs`, that record what was delivered to the pipeline database.

### PostgreSQL: 2 tables (`src/storage/schema_postgres.sql`)
| Table | Contents |
|---|---|
| `user_profiles` | Same as in SQLite (`changes` and `raw_json` as JSONB) |
| `posts` | Same as in SQLite, plus `media` (a JSONB list of the post's photos, videos and GIFs; `[]` if none) and `media_count` |

PostgreSQL has no separate state table. The newest stored post for each user is `MAX(post_id)`. User status and the run log are kept in SQLite and JSON, so PostgreSQL can't be the only enabled output.

**If PostgreSQL was unreachable for some runs,** nothing is fetched from X again for it. At the start of every run in which both SQLite and PostgreSQL are available, the program copies into PostgreSQL:
- **every post SQLite has and PostgreSQL doesn't**, including posts recovered from gaps, with its media, referenced text, raw JSON and original `first_seen_at`
- **profiles SQLite has newer**, from a refresh or status change PostgreSQL missed, or accounts added in the meantime. The two `changes` histories are combined, so no entry is lost.

Rows already in PostgreSQL are never changed. This costs no API credits, and `run.log` shows `PostgreSQL: copied N missing post(s) and M profile(s) from SQLite (no API cost).` If the copy fails, the error is logged, and PostgreSQL's missing posts are fetched from X again, as a fallback.

```sql
-- every media item with its post
SELECT p.post_id, p.username, m->>'type' AS type, m->>'url' AS url
FROM posts p, jsonb_array_elements(p.media) m;
```

### Profile changes (`user_profiles.changes`)
Each refresh (weekly, or at once for a new account) **overwrites** the account's single row with the latest data. Before that, it compares the stored values with the new ones, and **appends** an entry to `changes` for every tracked field that differs. Earlier entries are never removed.

| | Fields |
|---|---|
| **Tracked** | `username` (renames), `name`, `description` (bio), `location`, `url`, `verified`, `verified_type`, `protected`, `profile_image_url`, `pinned_tweet_id`, and our own `status` (for example `ok` → `suspended` / `not_found` / `renamed`) |
| **Not tracked** | follower, following, tweet and listed counts, because they change at every refresh. The latest values are always in the row. |

- **Change entries:** each one is `{"changed_at": "...", "field": "description", "old": "...", "new": "..."}`.
- **`last_changed_at`:** when the most recent change was seen.
- **`first_seen_at`:** when monitoring of the account began.
- **A new account starts with `changes = []`.**
- **Timing:** changes are noticed at the next refresh, so up to a week late. If a field changes twice between refreshes, only the first and final values are seen.

```sql
-- PostgreSQL: all profile changes in the last 30 days
SELECT p.username, c->>'field' AS field, c->>'old' AS old_value, c->>'new' AS new_value,
       (c->>'changed_at')::timestamptz AS changed_at
FROM user_profiles p, jsonb_array_elements(p.changes) AS c
WHERE (c->>'changed_at')::timestamptz >= now() - interval '30 days'
ORDER BY changed_at DESC;

-- accounts that changed anything this week
SELECT username, last_changed_at FROM user_profiles WHERE last_changed_at >= now() - interval '7 days';
```
```sql
-- SQLite: every change ever recorded
SELECT p.username, json_extract(c.value, '$.changed_at'), json_extract(c.value, '$.field'),
       json_extract(c.value, '$.old'), json_extract(c.value, '$.new')
FROM user_profiles p, json_each(p.changes) AS c;
```

### JSON (`data/json/`)
| File | Contents |
|---|---|
| `profile.json` | All users in one file. For each: `user_id`, `username`, status, the latest `profile` exactly as the API returned it, and the same `changes` list as the databases |
| `tweets.jsonl` | All tweets from all users, **one JSON object per line**, each with its `post_url`. New tweets are appended after each run. (The file was rewritten once to add `post_url` to lines written before that field existed.) Split lines only at `\n`; see the note below. |
| `_state.json` | Internal bookkeeping (newest stored tweet per user). Don't edit it; if it's deleted, it is rebuilt from `tweets.jsonl`. |
| `_runs.jsonl` | One summary line per run |

**Reading the tweets:**
```python
import pandas as pd
tweets = pd.read_json("data/json/tweets.jsonl", lines=True, dtype={"post_id": str, "author_id": str})
```
**If you read it with your own code,** split lines **only at `\n`**, for example `for line in open(path, encoding="utf-8", newline="\n")`. Don't use Python's `str.splitlines()`: tweet text can contain Unicode line separators (`U+2028`/`U+2029`), and `splitlines()` would cut a line in the middle of a tweet.
Group tweets by `author_id`, not `username`: if an account is renamed, its older lines keep the old username.

**Safety:**
- **Crashes:** if a run crashes while writing, the incomplete part of `tweets.jsonl` is removed on the next run, and those tweets are fetched again exactly once.
- **Damaged `profile.json`:** it is never overwritten. The JSON output is skipped, with an error in the log, until you fix or move the file.

### Notes
- **Retweets, replies and quotes:** `text` is stored exactly as the API returns it, so a retweet's text may be cut off with "…". The tweet it refers to is requested in the same call (`referenced_tweets.id` expansion) and stored on the post itself:
  - `referenced_full_text`: the **complete** text, including long posts over 280 characters
  - `referenced_author_id`: the referenced tweet's author (a numeric user id)
  - `referenced_created_at`: when it was posted

  For a retweet these describe the **original**, for a reply the **tweet replied to**, for a quote the **quoted tweet** (a post that is both a reply and a quote gets the replied-to tweet). They are empty when X can't return the tweet (deleted, protected or suspended), and for posts collected before this was added. Referenced tweets are never stored as rows of their own, so they never affect the cursor.
- **Cost of referenced tweets:** X bills each one as a post read ($0.005), about +27% on post cost for typical accounts. They are included in `est. cost` and in `post_reads`, but not in `posts_new`. X doesn't charge twice for the same tweet on the same UTC day, so `credits used` can be slightly below the estimate.
- **Metrics:** like and view counts are a snapshot from the moment the post was fetched.

## 6. Tests

```powershell
.venv\Scripts\python -m pytest -q
```
The tests use a fake API, so they cost nothing. To also run them against PostgreSQL, point `TEST_POSTGRES_DSN` at an **empty test database** first.
