# Contributing

Thanks for contributing to `digitaldu-backend-curation-service`. A few quick notes:

## Development setup

```sh
python3.12 -m venv .venv        # Python 3.12 — matches RHEL 8.10
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
# fill in .env values
python -m pytest tests/ -v
flask --app app run --host 0.0.0.0 --port 8185
```

Every `pip` / `pytest` / `flask` invocation should run with the venv active — `which python` should print `…/.venv/bin/python`.

## Coding style

- PEP 8, 4-space indent (2 for JSON/YAML/Markdown).
- Standard library first, third-party second, local (`config`, `lib`, `routes`) third.
- New routes go in a blueprint under `routes/`; the work itself goes in `lib/`. Route handlers validate input and shape the response — they don't hold storage or filesystem logic.
- Validate every path segment that comes off a request with `lib/safe_names.validate_segment` before it touches the filesystem, SFTP, or S3.
- No shell. Use `subprocess` with an argument list and `shell=False` — never `os.system` or a shell string.
- Log operational events in the existing `START` / per-item / `END` shape (see `lib/wasabi.py`) so `journalctl -u curation-api | grep` stays useful. Never log the API key, SFTP password, or AWS credentials.
- Pin new dependencies to an exact version in `requirements.txt` with a one-line comment saying why.

## Tests

All tests live flat under `tests/` and are `unittest.mock`-based — **no AWS credentials, network, or extra plugins**. A handful of legacy tests read module-level config at import time, so the suite needs dummy env values:

```sh
SFTP_REMOTE_PATH=/tmp/fake-sftp WASABI_PROFILE=test \
WASABI_ENDPOINT=https://example.com WASABI_BUCKET=test-bucket \
python -m pytest tests/ -v
```

`tests/smoke_test.sh` is a separate curl-based endpoint check that runs against a live instance — see the README.

Every PR should land tests. Mock the boto3 client and the paramiko transport; don't reach for real buckets or a real SFTP host.

## Commit style

One purpose per commit. Imperative subject. Reference issues with `#nnn`. Don't bundle refactors with feature work.

## Coordinating with repo-backend-v2

This service is one half of an ingest run — [repo-backend-v2](https://github.com/dulibrarytech/repo-backend-v2) is its only client. A change to a route's request shape, response shape, or status codes is a breaking change on the Node side: land the matching change there and note the pairing in the PR. Staff-facing wording belongs in repo-backend-v2, not here — this service ships codes and data.

## License

Apache 2.0. By contributing you agree your contribution is licensed under the same terms.
