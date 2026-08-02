# Digital Archives Manager @ DU Curation Service

## Table of Contents

* [README](#readme)
* [Architecture Overview](#architecture-overview)
* [Operations](#operations)
* [Releases](#releases)
* [Contact](#contact)

## README

### Background

Flask service that owns the **on-host filesystem + Wasabi S3 + SFTP** side of the University of Denver Libraries' digital repository ingest pipeline. It is called over HTTP from [repo-backend-v2](https://github.com/dulibrarytech/repo-backend-v2) (the Node.js ingest worker) and performs the storage and filesystem work that backend can't — or shouldn't — do directly: staging and QAing package folders on the Archivematica SFTP drop, creating ArchivesSpace digital objects, copying AIPs to the Wasabi storage tier, and minting the presigned URLs the staff dashboard serves. It is the sole holder of the Wasabi (boto3) credentials; the Node side carries only a URL and an API key. Deploy the two services together — an ingest run cannot complete without both.

### Contributing

Check out our [contributing guidelines](/CONTRIBUTING.md) for ways to offer feedback and contribute.

### Licenses

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

All other content is released under [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).

### Local Environment Setup

**Prerequisites**

* **Python 3.12** — matches the RHEL 8.10 application stream (`dnf install python3.12`).
* **System libraries** (RHEL 8.10):
  * `libmagic` (for `python-magic`): `dnf install file-libs`
  * `gcc` + `python3.12-devel` if a wheel falls through to a source build (rare on x86_64 — boto3 / paramiko / cryptography all ship binary wheels)
* **Wasabi credentials** — either `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` in the service env (preferred; no dependency on `~/.aws/*` files), or an AWS CLI profile at `~/.aws/config` for the service user matching `WASABI_PROFILE`. See [Wasabi credentials](#wasabi-credentials) for the resolution order.
* **Network egress** to the Archivematica SFTP host and the Wasabi endpoint. Optional (full functionality): ArchivesSpace for the digital-object CLI. Endpoint and health routes work without it.

**Install and configure**

```bash
cd /library/lib-sftp
git clone <repo-url> digitaldu-backend-curation-service
cd digitaldu-backend-curation-service

# Create a Python 3.12 virtualenv. The systemd unit looks for it at
# `.venv/bin/gunicorn` (see deploy/curation-api.service), so use that
# exact path.
python3.12 -m venv .venv

# Activate it. Every subsequent pip / pytest / flask invocation should
# be run with the venv active. To verify:
#   which python       # should print …/.venv/bin/python
#   python --version   # should print Python 3.12.x
source .venv/bin/activate

# RHEL 8.10's bundled pip is older than what most modern wheels prefer.
pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt` pulls:

- `Flask`, `Werkzeug`, `Flask-CORS`, `gunicorn` — web stack
- `python-dotenv` — `.env` loading
- `paramiko` — SFTP (replaces the unmaintained `pysftp`)
- `boto3` — Wasabi S3 (replaces the `aws s3 cp` shellout)
- `ArchivesSnake`, `python-magic` — ArchivesSpace-side tools

If you ever need to be explicit about the interpreter (cron, scripts, multiple Pythons on the box), use the module form — inside an activated venv, plain `pip` is already the right one:

```bash
python3.12 -m pip install -r requirements.txt
```

**Environment**

Copy `.env.example` to `.env` (dev) or write the same values to `/etc/curation-api/env` (prod — see the systemd unit).

```bash
cp .env.example .env
chmod 0600 .env
$EDITOR .env
```

Fill in:

- `API_KEY` — shared secret for the Node side; any high-entropy string. Generate with `openssl rand -hex 32`.
- `READY_PATH`, `INGEST_PATH` — local filesystem staging directories (`001-ready`, `002-ingest`).
- `SFTP_HOST`, `SFTP_ID`, `SFTP_PWD`, `SFTP_REMOTE_PATH` — Archivematica SFTP daemon address + credentials.
- `WASABI_ENDPOINT`, `WASABI_BUCKET` — Wasabi endpoint + the **batch archive** bucket (completed ingests' source packages). `WASABI_BUCKET` accepts the legacy `s3://bucket-name/` form (an optional path becomes a base key prefix) OR the bare `name` form.
- `WASABI_AIP_BUCKET` — the **AIP store** bucket/prefix (e.g. `s3://bucket-name/aip-store/`) used by the `/api/v2/aip/*` routes. Required for those routes; they refuse cleanly if unset rather than falling back to the batch bucket.
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_DEFAULT_REGION`) — Wasabi credentials, preferred form. Alternatively `WASABI_PROFILE` naming a profile in `~/.aws/config` for the service user (used only when the explicit keys are unset).
- `WORKSPACE`, `ASPACE_USERNAME`, `ASPACE_PASSWORD`, `SCRIPT_PATH`, `LOG_PATH` — ArchivesSpace-side config.
- `DEFAULT_URL` — ArchivesSpace API base URL, read straight from the environment by `lib/make_digital_object.py`. Required: without it that script exits 1 during validation and `/api/v1/astools/make-digital-objects` returns a 500 on every call. Optional companions: `TESTING_URL` (used when a request sets `test=true`; falls back to `DEFAULT_URL`, so test runs otherwise hit production) and `ASPACE_REPOSITORY_ID` (default `2`).

`config.py` validates the required ones (`API_KEY` today) at boot and raises `RuntimeError` listing any missing var. Per-endpoint env checks remain in the route handlers as a second line of defense.

<a name="wasabi-credentials"></a>
**Wasabi credentials**

Credential resolution order (`lib/wasabi._make_client`):

1. **Explicit env vars** — `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_DEFAULT_REGION`) from the service env. Preferred: no dependency on `~/.aws/*` existing for whichever user the process runs as.
2. **Named profile** — `WASABI_PROFILE` from `~/.aws/config` / `~/.aws/credentials`, used only when the env keys are unset:

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

3. **Neither** — loud `RuntimeError` at first use; there is no silent fallback to instance metadata or ambient host credentials.

Verify whichever path you configured with the on-demand health probe (`GET /health/wasabi`) — it exercises the exact same client construction the uploads use.

**Run**

Dev (Flask built-in server):

```bash
source .venv/bin/activate
flask --app app run --host 0.0.0.0 --port 8185
# → http://127.0.0.1:8185/health
```

OR run `app.py` directly (the `if __name__ == '__main__'` fallback):

```bash
source .venv/bin/activate
python app.py
```

Both forms reload on file changes only if `FLASK_DEBUG=1` is set. Don't use either in production — single-threaded, no graceful shutdown.

Prod (gunicorn under systemd) — the shipped unit at `deploy/curation-api.service` expects the project at `/library/lib-sftp/digitaldu-backend-curation-service/`, a venv at `<project>/.venv/`, an env file at `/etc/curation-api/env` (mode 0600, owned by `curation`), and a log directory `/var/log/curation-api/` writable by user `curation`:

```bash
sudo cp deploy/curation-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now curation-api
sudo systemctl status curation-api
sudo journalctl -u curation-api -f     # live logs
```

On boot you should see two log lines confirming a clean start:

```
INFO ... wasabi probe OK bucket=<name> elapsed_ms=<N>
INFO ... [<pid>] Listening at: http://127.0.0.1:8185
```

If the Wasabi probe says `FAILED`, the `err=` text names the specific reason (`ProfileNotFound`, `403`, `EndpointConnectionError`, …) — fix the config and `systemctl restart curation-api`.

`deploy/nginx.conf.example` is a starter template for the reverse proxy. Production deployments typically front this with nginx on `:443` proxying to `127.0.0.1:8185`.

**Tests**

Unit + integration (pytest), inside the activated venv. A handful of legacy tests read module-level config at import time, so give the suite dummy env values — any non-empty strings work, nothing touches the network:

```bash
# All tests
SFTP_REMOTE_PATH=/tmp/fake-sftp WASABI_PROFILE=test \
WASABI_ENDPOINT=https://example.com WASABI_BUCKET=test-bucket \
python -m pytest tests/ -v

# A single file
python -m pytest tests/test_wasabi.py -v

# Coverage of one module (requires `pip install coverage`)
coverage run -m pytest tests/test_wasabi.py
coverage report -m --include='lib/wasabi.py'
```

Every test file is `unittest.mock`-based — **no AWS credentials, network, or extra plugins needed**. Coverage spans the Wasabi client (upload + per-file `head_object` verification, bucket parser, credential resolution, health probe), the S3-only archive contract, the per-uuid rollback lock and pure-SFTP cleanup, path-segment validation and no-shell subprocess pins, batch structure QA and its route response shapes, the AIP copy/progress/list routes, Make Digital Objects failure propagation, SFTP connect retry and upload progress, and the audit/repair scripts.

Endpoint smoke test (curl) — `tests/smoke_test.sh` hits every endpoint and reports pass/fail:

```bash
# Read-only / safe endpoints (default)
API_KEY=<your-key> ./tests/smoke_test.sh

# Against a remote instance
API_KEY=<your-key> ./tests/smoke_test.sh --url https://curation-api.internal

# Include state-mutating endpoints (move-to-ingest etc.) — requires a
# test fixture you don't mind manipulating
API_KEY=<your-key> ./tests/smoke_test.sh --full --test-folder my_test_batch
```

Exit codes: `0` all passed, `1` one or more failed, `2` config error (missing API_KEY, service unreachable). Requires `curl` and `jq` on the box running the test.

Health-check from anywhere, without any tooling:

```bash
curl -s http://127.0.0.1:8185/health | jq
curl -s -H "X-API-Key: $API_KEY" http://127.0.0.1:8185/health/wasabi | jq
```

The Wasabi health probe is the fastest way to confirm the service can talk to S3 after a config change — no test runner needed.

### Maintainers

@freyesdulib

## Architecture Overview

A Flask application (blueprints + gunicorn under systemd) exposing a private HTTP API on `127.0.0.1:8185`, fronted by nginx in production. It holds no database of its own: state lives on the local filesystem (the staging directories), on the Archivematica SFTP drop, and in Wasabi S3. `repo-backend-v2` is the only client.

```
+---------------+   HTTP   +-----------------------+   SFTP    +---------------+
| repo-backend  | -------> | curation-api (this)   | --------> | Archivematica |
| -v2 (Node)    |          |                       |           |  SFTP daemon  |
+---------------+          |  +------ boto3 ------+|           +---------------+
                           |  | Wasabi S3:       | |
                           |  |  batch backup    | |
                           |  |  aip-store       | |
                           |  +------------------+ |
                           |  +------ local fs --+ |
                           |  | workspace        | |
                           |  | 001-ready ─┐     | |
                           |  | 002-ingest │ ←── | |
                           |  +------------------+ |
                           +-----------------------+
```

### External services

| Service | Role |
| --- | --- |
| **[repo-backend-v2](https://github.com/dulibrarytech/repo-backend-v2)** (Node) | The only caller. Drives every route as part of the pre-ingest workspace and the six-stage ingest pipeline. Holds `CURATION_API` + `CURATION_API_KEY`, nothing else. |
| **Archivematica SFTP daemon** | Ingest drop target. Staged batches are pushed to the `internal-sftp` subsystem via paramiko; cleanup removes a package's files and parent dir after success. |
| **Wasabi S3 — batch backup** | Post-ingest backup of the `002-ingest` staging copy (`WASABI_BUCKET`). |
| **Wasabi S3 — AIP store** | Storage tier for Archivematica AIPs (`WASABI_AIP_BUCKET`), plus the presigned download URLs the dashboard serves. |
| **ArchivesSpace** | Digital-object creation via the bundled `lib/make_digital_object.py` CLI; `uri.txt` per package is the output. |
| **Local filesystem** | `WORKSPACE` (staff-assembled batches), `READY_PATH` (`001-ready`), `INGEST_PATH` (`002-ingest`). |

### Ingest role

Where this service sits in `repo-backend-v2`'s six-stage pipeline:

```
Pre-ingest workspace → /api/v1/astools/*  (batch listing, structure QA, Make Digital Objects)
Stage 2 (upload)     → /api/v2/qa/*       (move-to-ingest, move-to-sftp, upload-status)
Stage 5 (repository) → /api/v2/qa/*       (cleanup_sftp, move-to-ingested → batch archive)
Stage 6 (aip_store)  → /api/v2/aip/*      (copy-to-wasabi, presigned-url)
```

At the end of a successful ingest, the batch's `002-ingest` staging copy is backed up **straight to Wasabi** — every uploaded file is verified with a `head_object` size check, and the staging copy is removed only after that verification passes.

#### Health endpoints (no auth)

| Endpoint | Purpose |
|---|---|
| `GET /` | Returns `APP_VERSION` |
| `GET /health` | `{"status": "ok", "version": ...}` |
| `GET /health/wasabi` | On-demand Wasabi `head_bucket` probe |

<a name="batch-structure-qa"></a>
### Batch structure QA

Staff assemble batches by hand (`WORKSPACE/new_<collection>-resources_<N>/<package>/<files>`). `lib/batch_structure.py` scans every batch in a single `os.scandir` pass per directory — no file is ever opened, so 100-package / hundreds-of-files batches scan in milliseconds — and the `/workspace` + `/workspace/packages` responses carry the results as `structure_errors` entries:

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

`items` lists are capped at 20 entries (`total` carries the true count). The server ships codes only — **all staff-facing wording lives in repo-backend-v2** (`ingester/libs/structure_flags.js`), which renders the notices in the Make Digital Objects view and blocks the MDO / Submit actions on error-severity flags (server-enforced there, too).

## Operations

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

Every uploaded file is immediately re-checked with `head_object` (size comparison). A `wasabi VERIFY FAILED` line means the remote object's size disagreed with the local file — that file counts as **failed**, `ok` comes back `False`, and the `002-ingest` staging copy is deliberately left in place for a re-run. On the Node side this surfaces as a FAILED "Archive to Wasabi" row in the dashboard's Job History view.

### Verify objects landed in Wasabi

The AWS CLI is not installed on the curation host; use the service's own venv + credentials (same resolution as the uploads):

```bash
.venv/bin/python -c "import config; from lib import wasabi; \
c = wasabi._make_client(); b, p = wasabi._parse_bucket(config.WASABI_BUCKET); \
r = c.list_objects_v2(Bucket=b, Prefix=p + '<folder>/', MaxKeys=25); \
[print(o['Key'], o['Size']) for o in r.get('Contents', [])]"
```

For a full per-file audit of a batch, use `scripts/reconcile_ingested_wasabi.py --batch <folder>` instead. Companion scripts under `scripts/`: `sync_missing_to_wasabi.py` (re-upload what's missing), `restore_ingested_from_wasabi.py` (pull an archived batch back to disk), and `abort_stale_multipart_uploads.py` (clear orphaned multipart parts).

### Rebuild the venv from scratch

If pip + lockfile drift, broken symlinks (e.g., venv created on a different box), or a Python version bump:

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

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `RuntimeError: Missing required environment variables: API_KEY` at startup | `.env` not loaded or `API_KEY` blank | Ensure `EnvironmentFile=/etc/curation-api/env` exists and is readable by user `curation`, or for dev that `.env` is present in CWD. |
| `wasabi probe FAILED err=ProfileNotFound` | `WASABI_PROFILE` doesn't match an entry in `~/.aws/config` for the service user | Check `cat ~curation/.aws/config`; profile name is case-sensitive. |
| `wasabi probe FAILED err=head_bucket failed (403)` | Wasabi creds wrong OR bucket policy denies | Re-issue keys; verify with `aws s3 ls --profile=<p> --endpoint-url=<e> <bucket>` as user `curation`. |
| `wasabi upload FAILED file=... err=NoCredentialsError` | Profile creds disappeared mid-run (token expiry, file rotation) | Restart the service to re-read `~/.aws/config`. |
| `wasabi VERIFY FAILED file=... local=N remote=M` | Uploaded object's size disagrees with the local file (truncated/partial transfer) | The staging copy is preserved — fix the cause and re-run the archive (re-trigger `move-to-ingested`, or `scripts/sync_missing_to_wasabi.py`). Also appears as a FAILED "Archive to Wasabi" row in the dashboard Job History. |
| Make Digital Objects reports FAILED with `Multiple objects with component ID "..." found` | Duplicate component ID in ArchivesSpace — two archival objects share the package's ID, so `uri.txt` can't be generated for that package | Fix the duplicate in ArchivesSpace, then run Make Digital Objects again (already-succeeded packages are unaffected). Since 2026-07-27 the run exits non-zero and the dashboard card names the failed packages; previously it reported success and the gap surfaced only at Description QA. |
| `aws s3 cp` no longer in process list | Expected — boto3 replaced the CLI shellout. Look for `wasabi upload` log lines instead. | — |
| paramiko `AuthenticationException` | Wrong `SFTP_PWD` OR Archivematica restricted user account | Verify with `sftp -P <port> $SFTP_ID@$SFTP_HOST`. |
| `python-magic` import error | `libmagic` system library missing | `dnf install file-libs`. |
| 401 / 403 from every request | API key mismatch | Ensure the Node side's `CURATION_API_KEY` matches this service's `API_KEY`. |
| 500 with `Server configuration error` | `API_KEY` env var is unset on this server | See first row. |
| `which pip` doesn't point to `.venv/bin/pip` | Venv not activated | `source .venv/bin/activate` from project root. |
| `pip install` fails on a wheel build | Missing build deps | `dnf install gcc python3.12-devel openssl-devel libffi-devel`; rare on x86_64. |

## Releases

* curation-api 1.0.0

## Contact

Ways to get in touch:

* Fernando Reyes (Developer at University of Denver) - fernando.reyes@du.edu
* Create an issue in this repository
