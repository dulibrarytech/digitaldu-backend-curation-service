# digitaldu-backend-curation-service

Flask service that owns the **on-host filesystem + Wasabi S3 + SFTP**
side of the DigitalDU ingest pipeline. Called from
`repo-backend-v2` (the Node.js ingest worker) over HTTP; talks to:

- the local filesystem (workspace, `001-ready`, `002-ingest` staging),
- the Archivematica SFTP daemon (`internal-sftp` subsystem) via paramiko,
- Wasabi S3 via boto3 (two buckets: the batch archive and the AIP store),
- ArchivesSpace via the bundled `make_digital_object.py` CLI.

It supersedes the legacy `digitaldu-backend-qa` (QA service) and
`astools-web_v2` services — both surfaces live here behind a single
Flask app.

```
+---------------+   HTTP   +-----------------------+   SFTP    +---------------+
| repo-backend  | -------> | curation-api (this)   | --------> | Archivematica |
| -v2 (Node)    |          |                       |           |  SFTP daemon  |
+---------------+          |  +------ boto3 ------+|           +---------------+
                           |  | Wasabi S3:       | |
                           |  |  batch archive   | |
                           |  |  aip-store       | |
                           |  +------------------+ |
                           |  +------ local fs --+ |
                           |  | workspace        | |
                           |  | 001-ready ─┐     | |
                           |  | 002-ingest │ ←── | |
                           |  +------------------+ |
                           +-----------------------+
```

At the end of a successful ingest, the batch's `002-ingest` staging copy
is archived **straight to Wasabi** — every uploaded file is verified with
a `head_object` size check, and the staging copy is removed only after
that verification passes. 

## Requirements

- **Python 3.12** (matches RHEL 8.10 application stream — `dnf install python3.12`)
- **System libraries** (RHEL 8.10):
  - `libmagic` (for `python-magic`): `dnf install file-libs`
  - `gcc` + `python3.12-devel` if any wheel falls through to source build
    (rare on x86_64 — boto3 / paramiko / cryptography all ship binary wheels)
- **Wasabi credentials** — either `AWS_ACCESS_KEY_ID` +
  `AWS_SECRET_ACCESS_KEY` in the service env (preferred; no dependency
  on `~/.aws/*` files), or an AWS CLI profile at `~/.aws/config` for
  the service user matching `WASABI_PROFILE`. See "Wasabi credentials"
  below for the resolution order.
- **Network egress** to the Archivematica SFTP host and the Wasabi
  endpoint.

## Project layout

```
digitaldu-backend-curation-service/
├── app.py                  Flask app factory + entry point + startup probe
├── auth.py                 X-API-Key + ?api_key= decorators (two response shapes)
├── config.py               Env-var loader + startup validation
├── requirements.txt        Pinned Python deps
├── .env.example            Template for .env (gitignored at deploy)
├── lib/
│   ├── archivematica_ops.py    SFTP + local filesystem ops (ready/ingest staging)
│   ├── archivesspace_ops.py    ASpace tools (workspace / processed / uri.txt checks)
│   ├── batch_structure.py      Batch structure QA scan (workspace listing flags)
│   ├── aip_ops.py              AIP-store copy (AM Storage Service → Wasabi aip bucket)
│   ├── wasabi.py               boto3 S3 upload (per-file verified) + health check
│   ├── safe_names.py           Path-segment validation (traversal guard)
│   └── make_digital_object.py  CLI launched by astools route
├── routes/
│   ├── qa.py                   /api/v2/qa/* — Archivematica-side endpoints
│   ├── astools.py              /api/v1/astools/* — ArchivesSpace-side endpoints
│   └── aip.py                  /api/v2/aip/* — AIP-store endpoints
├── scripts/
│   ├── reconcile_ingested_wasabi.py   Read-only local-vs-Wasabi batch audit
│   └── sync_missing_to_wasabi.py      Uploads files the audit found missing
├── tests/                  pytest suite (see "Testing" below)
│   └── smoke_test.sh       curl-based endpoint sweep
└── deploy/
    ├── curation-api.service            systemd unit
    ├── logrotate.curation-api          log rotation
    └── nginx.conf.example              reverse-proxy template
```

## Endpoints (summary)

Two blueprints, both auth-required (`X-API-Key` header, or legacy
`?api_key=` query string).

### `/api/v2/qa/*` — Archivematica side

| Endpoint | Purpose |
|---|---|
| `GET /list-ready-folders` | List folders in `READY_PATH` |
| `GET /package-names` | Packages inside a ready folder |
| `GET /check-package-names`, `/check-file-names`, `/check-uri-txt` | Validation helpers |
| `GET /get-total-batch-size`, `/package-file-count` | Size / count helpers |
| `GET /move-to-ingest` | Move a package from `001-ready` to `002-ingest` |
| `GET /move-from-ingest-to-ready` | Rollback move (used by Node rollback flow) |
| `GET /move-to-sftp` | Push staged batch to AM's SFTP source |
| `GET /upload-status` | Poll SFTP upload progress |
| `GET /move-to-ingested` | After-success archive of `002-ingest/<uuid>/` to Wasabi (per-file verified; staging copy removed only on verified success). |
| `GET /cleanup_sftp` | Remove a package's files + parent dir from SFTP |
| `GET /reset_permissions` | chown ready folder back to service user |
| `GET /set-collection-folder`, `/check-collection-folder` | Folder naming helpers |
| `GET /get-uri-txt` | Read uri.txt from a package |

### `/api/v1/astools/*` — ArchivesSpace side

| Endpoint | Purpose |
|---|---|
| `GET /workspace` | List batches awaiting Make Digital Objects, **with structure-QA flags** (see "Batch structure QA" below). Returns batch objects `{name, packages, processed, structure_errors}` — malformed batches are included and flagged rather than silently skipped (exception: completely empty folders stay hidden until staff put anything in them). |
| `GET /workspace/packages` | Package names in one batch (`result` = sorted name array) + piggybacked `processed` and `structure_errors` |
| `GET /workspace/packages/files` | Per-package file listings |
| `GET /processed` | List batches with `uri.txt` (scan bounded to `batch/package/uri.txt` depth) |
| `GET /workspace/uri`, `/check-uri-txt` | URI helpers |
| `GET /move-to-ready` | Move a workspace batch into `001-ready` |
| `POST /make-digital-objects` | Launch the ASpace digital-object creation CLI |
| `POST /revert-to-make-digital-objects` | Delete `uri.txt` from every package (returns the batch to the MDO view) |

### `/api/v2/aip/*` — AIP store

| Endpoint | Purpose |
|---|---|
| `POST /copy-to-wasabi` | Stream an AM Storage Service AIP into the `WASABI_AIP_BUCKET` (idempotent via size-checked `head_object` probe) |
| `POST /presigned-url` | Mint a presigned GET URL for an AIP download |

### Health endpoints (no auth)

| Endpoint | Purpose |
|---|---|
| `GET /` | Returns `APP_VERSION` |
| `GET /health` | `{"status": "ok", "version": ...}` |
| `GET /health/wasabi` | On-demand Wasabi `head_bucket` probe |

## Batch structure QA

Staff assemble batches by hand
(`WORKSPACE/new_<collection>-resources_<N>/<package>/<files>`), and
structural mistakes used to be invisible or silently destructive (see
`repo/BATCH_PACKAGING_QA_FINDINGS.md`). `lib/batch_structure.py` scans
every batch in a single `os.scandir` pass per directory — no file is
ever opened, so 100-package / hundreds-of-files batches scan in
milliseconds — and the `/workspace` + `/workspace/packages` responses
carry the results as `structure_errors` entries:

```json
{"code": "loose_files", "severity": "error",
 "items": ["scan1.tif", "..."], "total": 41}
```

| Code | Severity | Meaning |
|---|---|---|
| `no_packages` | error | Batch folder has no package subfolders |
| `loose_files` | error | Files sit directly in the batch folder |
| `empty_package` | error | Package folder has no content files |
| `nested_dirs` | error | Package folder contains subfolders |
| `bad_folder_name` | error | Name breaks the `new_…-resources_<N>` convention |
| `unreadable` | error | Permission denied scanning the batch |
| `partially_processed` | info | Some but not all packages have `uri.txt` |
| `name_hygiene` | warn | Spaces in package/file names |

`items` lists are capped at 20 entries (`total` carries the true
count). The server ships codes only — **all staff-facing wording lives
in repo-backend-v2** (`ingester/libs/structure_flags.js`), which renders
the notices in the Make Digital Objects view and blocks the MDO /
Submit actions on error-severity flags (server-enforced there, too).

## Setup

### 1. Clone + virtualenv

```bash
cd /library/lib-sftp
git clone <repo-url> digitaldu-backend-curation-service
cd digitaldu-backend-curation-service

# Create a Python 3.12 virtualenv. The systemd unit looks for it at
# `.venv/bin/gunicorn` (see deploy/curation-api.service), so use that
# exact path.
python3.12 -m venv .venv

# Activate it. Every subsequent pip / pytest / flask invocation
# should be run with the venv active. To verify:
#   which python   # should print …/.venv/bin/python
#   python --version   # should print Python 3.12.x
source .venv/bin/activate

# Upgrade pip itself before installing — RHEL 8.10's bundled pip
# is older than what most modern wheels prefer.
pip install --upgrade pip
```

### 2. Install dependencies

Inside the activated venv:

```bash
pip install -r requirements.txt
```

This pulls:

- `Flask`, `Werkzeug`, `Flask-CORS`, `gunicorn` — web stack
- `python-dotenv` — `.env` loading
- `paramiko` — SFTP (replaces the unmaintained `pysftp`)
- `boto3` — Wasabi S3 (replaces `aws s3 cp` shellout)
- `ArchivesSnake`, `python-magic` — ArchivesSpace-side tools

If you ever need to be explicit about the Python interpreter (cron,
scripts, multiple Pythons on the box), use the module form:

```bash
python3.12 -m pip install -r requirements.txt
```

Inside an activated venv, plain `pip` is already the right one.

### 3. Configure environment

Copy `.env.example` to `.env` (dev) or write the same values to
`/etc/curation-api/env` (prod — see the systemd unit).

```bash
cp .env.example .env
chmod 0600 .env
$EDITOR .env
```

Fill in:

- `API_KEY` — shared secret for the Node side; any high-entropy
  string. Generate with `openssl rand -hex 32`.
- `READY_PATH`, `INGEST_PATH` — local filesystem staging directories
  (`001-ready`, `002-ingest`).
- `SFTP_HOST`, `SFTP_ID`, `SFTP_PWD`, `SFTP_REMOTE_PATH` —
  Archivematica SFTP daemon address + creds.
- `WASABI_ENDPOINT`, `WASABI_BUCKET` — Wasabi endpoint + the **batch
  archive** bucket (completed ingests' source packages).
  `WASABI_BUCKET` accepts the legacy `s3://name/` form (an optional
  path becomes a base key prefix) OR the bare `name` form.
- `WASABI_AIP_BUCKET` — the **AIP store** bucket/prefix (e.g.
  `s3://repository-bucket/aip-store/`) used by the `/api/v2/aip/*`
  routes. Required for those routes; they refuse cleanly if unset
  rather than falling back to the batch bucket.
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
  (+ optional `AWS_DEFAULT_REGION`) — Wasabi credentials, preferred
  form. Alternatively `WASABI_PROFILE` naming a profile in
  `~/.aws/config` for the service user (used only when the explicit
  keys are unset).
- `WORKSPACE`, `ASPACE_USERNAME`, `ASPACE_PASSWORD`, `SCRIPT_PATH`,
  `LOG_PATH` — ArchivesSpace-side config.

`config.py` validates the required ones (`API_KEY` today) at boot
and raises `RuntimeError` listing any missing var. Per-endpoint env
checks remain in the route handlers as a second line of defense.

### 4. Wasabi credentials

Credential resolution order (`lib/wasabi._make_client`):

1. **Explicit env vars** — `AWS_ACCESS_KEY_ID` +
   `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_DEFAULT_REGION`) from the
   service env. Preferred: no dependency on `~/.aws/*` existing for
   whichever user the process runs as.
2. **Named profile** — `WASABI_PROFILE` from `~/.aws/config` /
   `~/.aws/credentials`, used only when the env keys are unset:

   ```ini
   # ~/.aws/config
   [profile wasabi-prod]
   region = us-east-1
   output = json

   # ~/.aws/credentials
   [wasabi-prod]
   aws_access_key_id     = <key>
   aws_secret_access_key = <secret>
   ```

3. **Neither** — loud `RuntimeError` at first use; there is no silent
   fallback to instance metadata or ambient host credentials.

Verify whichever path you configured with the on-demand health probe
(`GET /health/wasabi`, below) — it exercises the exact same client
construction the uploads use.

## Running

### Dev (Flask built-in server)

```bash
source .venv/bin/activate
flask --app app run --host 0.0.0.0 --port 8185
```

OR run `app.py` directly (the `if __name__ == '__main__'` fallback):

```bash
source .venv/bin/activate
python app.py
```

Both forms reload on file changes only if `FLASK_DEBUG=1` is set.
Don't use this in production — single-threaded, no graceful shutdown.

### Prod (gunicorn under systemd)

The shipped unit at `deploy/curation-api.service` expects:

- Project checked out at `/library/lib-sftp/digitaldu-backend-curation-service/`
- Venv at `<project>/.venv/`
- Env file at `/etc/curation-api/env` (mode 0600, owned by `curation`)
- Log directory `/var/log/curation-api/` writable by user `curation`

Install + start:

```bash
sudo cp deploy/curation-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now curation-api
sudo systemctl status curation-api
```

Live logs:

```bash
sudo journalctl -u curation-api -f
```

On boot you should see two log lines confirming the service came up cleanly:

```
INFO ... wasabi probe OK bucket=<name> elapsed_ms=<N>
INFO ... [<pid>] Listening at: http://127.0.0.1:8185
```

If the Wasabi probe says `FAILED`, the `err=` text names the
specific reason (`ProfileNotFound`, `403`, `EndpointConnectionError`,
…) — fix the config and `systemctl restart curation-api`.

### Reverse proxy

`deploy/nginx.conf.example` is a starter template. Production
deployments typically front this with nginx on `:443` and proxy to
`127.0.0.1:8185`.

## Testing

### Unit + integration (pytest)

Inside the activated venv. A handful of legacy tests read module-level
config at import time, so give the suite dummy env values (any
non-empty strings work — nothing touches the network):

```bash
# All tests
SFTP_REMOTE_PATH=/tmp/fake-sftp WASABI_PROFILE=test \
WASABI_ENDPOINT=https://example.com WASABI_BUCKET=test-bucket \
python -m pytest tests/ -v

# A single file
python -m pytest tests/test_wasabi.py -v

# Show coverage of the wasabi module (requires `pip install coverage`)
coverage run -m pytest tests/test_wasabi.py
coverage report -m --include='lib/wasabi.py'
```

Test files (all `unittest.mock`-based; **no AWS credentials, network,
or extra plugins needed**):

- `test_wasabi.py` — upload + per-file head_object verification,
  bucket parser, credential resolution, health probe.
- `test_move_to_ingested_phase3.py` — the S3-only archive contract
  (verified upload gates staging cleanup; no local archive copy).
- `test_move_from_ingest_to_ready.py` — per-uuid lock, pure-SFTP
  cleanup, rollback move-back flow.
- `test_qa_command_injection.py` — path-segment validation + no-shell
  subprocess pins.
- `test_batch_structure.py`, `test_astools_structure_routes.py`,
  `test_ready_stage_structure_fixes.py` — batch structure QA scan,
  route response shapes, and the ready-stage loose-file fixes.
- `test_reconcile_ingested_wasabi.py`, `test_sync_missing_to_wasabi.py`
  — the audit/repair scripts.
- `test_upload_progress.py` — background SFTP put + progress polling.

### Endpoint smoke test (curl)

`tests/smoke_test.sh` hits every endpoint and reports pass/fail.

```bash
# Read-only / safe endpoints (default)
API_KEY=<your-key> ./tests/smoke_test.sh

# Against a remote instance
API_KEY=<your-key> ./tests/smoke_test.sh --url https://curation-api.internal

# Include state-mutating endpoints (move-to-ingest etc.) — requires
# a test fixture you don't mind manipulating
API_KEY=<your-key> ./tests/smoke_test.sh --full --test-folder my_test_batch
```

Exit codes: `0` all passed, `1` one or more failed, `2` config
error (missing API_KEY, service unreachable).

Required tools on the box running the test: `curl`, `jq`.

### Health-check from anywhere

Without any tooling:

```bash
curl -s http://127.0.0.1:8185/health | jq
curl -s -H "X-API-Key: $API_KEY" http://127.0.0.1:8185/health/wasabi | jq
```

The Wasabi health probe is the fastest way to confirm the service
can talk to S3 after a config change — no test runner needed.

## Common operational tasks

### Watch Wasabi upload activity

```bash
sudo journalctl -u curation-api -f | grep -i wasabi
```

For any successful ingest you'll see:

```
wasabi upload START source=/data/ingest/<uuid>/ bucket=<name> prefix=<folder>/
wasabi upload file=objects/foo.tif size=5242880 → s3://<name>/<folder>/objects/foo.tif
wasabi upload <folder>/objects/foo.tif — 25% (1310720/5242880 bytes)
...
wasabi upload END uploaded=N verified=N failed=0 bytes=<total> elapsed_ms=<N> ok=True
```

Every uploaded file is immediately re-checked with `head_object`
(size comparison). A `wasabi VERIFY FAILED` line means the remote
object's size disagreed with the local file — that file counts as
**failed**, `ok` comes back `False`, and the `002-ingest` staging
copy is deliberately left in place for a re-run. On the Node side
this surfaces as a FAILED "Archive to Wasabi" row in the dashboard's
Job History view.

### Verify objects landed in Wasabi

The AWS CLI is not installed on the curation host; use the service's
own venv + credentials (same resolution as the uploads):

```bash
.venv/bin/python -c "import config; from lib import wasabi; \
c = wasabi._make_client(); b, p = wasabi._parse_bucket(config.WASABI_BUCKET); \
r = c.list_objects_v2(Bucket=b, Prefix=p + '<folder>/', MaxKeys=25); \
[print(o['Key'], o['Size']) for o in r.get('Contents', [])]"
```

For a full per-file audit of a batch, use the reconciliation script
(next section) with `--batch <folder>` instead.


### Rebuild the venv from scratch

If pip + lockfile drift, broken symlinks (e.g., venv created on a
different box), or a Python version bump:

```bash
sudo systemctl stop curation-api
rm -rf .venv
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
sudo systemctl start curation-api
sudo journalctl -u curation-api -n 50  # confirm clean boot
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `RuntimeError: Missing required environment variables: API_KEY` at startup | `.env` not loaded or `API_KEY` blank | Ensure `EnvironmentFile=/etc/curation-api/env` exists and is readable by user `curation`, or for dev that `.env` is present in CWD. |
| `wasabi probe FAILED err=ProfileNotFound` | `WASABI_PROFILE` doesn't match an entry in `~/.aws/config` for the service user | Check `cat ~curation/.aws/config`; profile name is case-sensitive. |
| `wasabi probe FAILED err=head_bucket failed (403)` | Wasabi creds wrong OR bucket policy denies | Re-issue keys; verify with `aws s3 ls --profile=<p> --endpoint-url=<e> <bucket>` as user `curation`. |
| `wasabi upload FAILED file=... err=NoCredentialsError` | Profile creds disappeared mid-run (token expiry, file rotation) | Restart the service to re-read `~/.aws/config`. |
| `wasabi VERIFY FAILED file=... local=N remote=M` | Uploaded object's size disagrees with the local file (truncated/partial transfer) | The staging copy is preserved — fix the cause and re-run the archive (re-trigger `move-to-ingested`, or `scripts/sync_missing_to_wasabi.py`). Also appears as a FAILED "Archive to Wasabi" row in the dashboard Job History. |
| `aws s3 cp` no longer in process list | Expected — boto3 replaced the CLI shellout. Look for `wasabi upload` log lines instead. | — |
| paramiko `AuthenticationException` | Wrong `SFTP_PWD` OR Archivematica restricted user account | Verify with `sftp -P <port> $SFTP_ID@$SFTP_HOST`. |
| `python-magic` import error | `libmagic` system library missing | `dnf install file-libs`. |
| 401 / 403 from every request | API key mismatch | Ensure the Node side's `CURATION_API_KEY` matches this service's `API_KEY`. |
| 500 with `Server configuration error` | `API_KEY` env var is unset on this server | See first row. |
| `which pip` doesn't point to `.venv/bin/pip` | Venv not activated | `source .venv/bin/activate` from project root. |
| `pip install` fails on a wheel build | Missing build deps | `dnf install gcc python3.12-devel openssl-devel libffi-devel`; rare on x86_64. |

## Licensing

Apache License, Version 2.0. See `LICENSE`.

Copyright 2026 University of Denver.
