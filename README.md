# digitaldu-backend-curation-service

Flask service that owns the **on-host filesystem + Wasabi S3 + SFTP**
side of the DigitalDU ingest pipeline. Called from
`repo-backend-v2` (the Node.js ingest worker) over HTTP; talks to:

- the local filesystem (`/data/ready`, `/data/ingest`, `/data/ingested`),
- the Archivematica SFTP daemon (`internal-sftp` subsystem) via paramiko,
- Wasabi S3 via boto3,
- ArchivesSpace via the bundled `make_digital_object.py` CLI.

It supersedes the legacy `digitaldu-backend-qa` (QA service) and
`astools-web_v2` services — both surfaces live here behind a single
Flask app.

```
+---------------+   HTTP   +-----------------------+   SFTP    +---------------+
| repo-backend  | -------> | curation-api (this)   | --------> | Archivematica |
| -v2 (Node)    |          |                       |           |  SFTP daemon  |
+---------------+          |  +------ boto3 ------+|           +---------------+
                           |  | Wasabi S3 upload | |
                           |  +------------------+ |
                           |  +------ local fs --+ |
                           |  | 001-ready ─┐     | |
                           |  | 002-ingest │ ←── | |
                           |  | 003-ingested     | |
                           |  +------------------+ |
                           +-----------------------+
```

## Requirements

- **Python 3.12** (matches RHEL 8.10 application stream — `dnf install python3.12`)
- **System libraries** (RHEL 8.10):
  - `libmagic` (for `python-magic`): `dnf install file-libs`
  - `gcc` + `python3.12-devel` if any wheel falls through to source build
    (rare on x86_64 — boto3 / paramiko / cryptography all ship binary wheels)
- **AWS CLI profile** at `~/.aws/config` for the service user, matching
  the `WASABI_PROFILE` env value. boto3 reads creds from this file.
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
│   ├── archivematica_ops.py    SFTP + local filesystem ops
│   ├── archivesspace_ops.py    ASpace tools (workspace / processed / make-digital-objects)
│   ├── wasabi.py               boto3 S3 upload + health check
│   └── make_digital_object.py  CLI launched by astools route
├── routes/
│   ├── qa.py                   /api/v2/qa/* — Archivematica-side endpoints
│   └── astools.py              /api/v1/astools/* — ArchivesSpace-side endpoints
├── tests/
│   ├── test_move_from_ingest_to_ready.py   pytest — SFTP cleanup + lock
│   ├── test_wasabi.py                      pytest — boto3 upload + health probe
│   └── smoke_test.sh                       curl-based endpoint sweep
└── deploy/
    ├── curation-api.service                systemd unit
    ├── logrotate.curation-api              log rotation
    └── nginx.conf.example                  reverse-proxy template
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
| `GET /move-to-ingested` | After-success archive to `003-ingested` + Wasabi S3 |
| `GET /cleanup_sftp` | Remove a package's files + parent dir from SFTP |
| `GET /reset_permissions` | chown ready folder back to service user |
| `GET /set-collection-folder`, `/check-collection-folder` | Folder naming helpers |
| `GET /get-uri-txt` | Read uri.txt from a package |

### `/api/v1/astools/*` — ArchivesSpace side

| Endpoint | Purpose |
|---|---|
| `GET /workspace` | List packages in workspace |
| `GET /workspace/packages`, `/workspace/packages/files` | Package + file details |
| `GET /processed` | List processed packages (have `uri.txt`) |
| `GET /workspace/uri`, `/check-uri-txt` | URI helpers |
| `GET /move-to-ready` | Move workspace package into ready |
| `POST /make-digital-objects` | Launch the ASpace digital-object creation CLI |

### Health endpoints (no auth)

| Endpoint | Purpose |
|---|---|
| `GET /` | Returns `APP_VERSION` |
| `GET /health` | `{"status": "ok", "version": ...}` |
| `GET /health/wasabi` | On-demand Wasabi `head_bucket` probe |

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
- `READY_PATH`, `INGEST_PATH`, `INGESTED_PATH` — local filesystem
  staging directories.
- `SFTP_HOST`, `SFTP_ID`, `SFTP_PWD`, `SFTP_REMOTE_PATH` —
  Archivematica SFTP daemon address + creds.
- `WASABI_ENDPOINT`, `WASABI_BUCKET`, `WASABI_PROFILE` — Wasabi
  details. `WASABI_BUCKET` accepts the legacy `s3://name/` form OR
  the bare `name` form. `WASABI_PROFILE` must match a profile in
  `~/.aws/config` for the service user.
- `WORKSPACE`, `ASPACE_USERNAME`, `ASPACE_PASSWORD`, `SCRIPT_PATH`,
  `LOG_PATH` — ArchivesSpace-side config.

`config.py` validates the required ones (`API_KEY` today) at boot
and raises `RuntimeError` listing any missing var. Per-endpoint env
checks remain in the route handlers as a second line of defense.

### 4. Wasabi credentials

boto3 reads `~/.aws/config` via the named profile. Create or verify
the entry for the service user:

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

Then set `WASABI_PROFILE=wasabi-prod` in `.env` and verify:

```bash
aws s3 ls --profile wasabi-prod --endpoint-url $WASABI_ENDPOINT $WASABI_BUCKET
```

If that command works, boto3 will too.

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

Inside the activated venv:

```bash
# All tests
python -m pytest tests/ -v

# A single file
python -m pytest tests/test_wasabi.py -v

# A single test
python -m pytest tests/test_wasabi.py::UploadDirectoryTest::test_happy_path_uploads_all_files_skipping_dotfiles -v

# Show coverage of the wasabi module (requires `pip install coverage`)
coverage run -m pytest tests/test_wasabi.py
coverage report -m --include='lib/wasabi.py'
```

Test files:

- `tests/test_move_from_ingest_to_ready.py` — exercises the
  per-uuid lock, the pure-SFTP `clean_up_sftp` rewrite, and the
  rollback move-back flow. paramiko's SFTP is mocked.
- `tests/test_wasabi.py` — boto3 upload + bucket parser + health
  probe + the `move_to_s3` shim's 0/1 contract. boto3 is mocked at
  the `_make_client` boundary — **no AWS credentials or network
  access needed to run**.

Both files use `unittest.mock` directly; no `moto` / `pytest-mock` /
other plugins required.

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
wasabi upload file=METS.xml size=12345 → s3://<name>/<folder>/METS.xml
wasabi upload file=objects/foo.tif size=5242880 → s3://<name>/<folder>/objects/foo.tif
wasabi upload <folder>/objects/foo.tif — 25% (1310720/5242880 bytes)
...
wasabi upload END uploaded=N failed=0 bytes=<total> elapsed_ms=<N> ok=True
```

### Verify objects landed in Wasabi

```bash
aws s3 ls --profile $WASABI_PROFILE --endpoint-url $WASABI_ENDPOINT \
  s3://$BUCKET/$FOLDER/
```

### Re-run the Wasabi health probe after a config change

Either restart the service (boot probe re-runs) or hit the
on-demand route:

```bash
curl -s -H "X-API-Key: $API_KEY" http://127.0.0.1:8185/health/wasabi | jq
```

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
