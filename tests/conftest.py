# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Shared pytest bootstrap for the curation-service test suite.

config.py reads its WASABI_* / API_KEY values from os.environ at import
time, and the first test module that (directly or transitively) does
`import config` freezes those values in sys.modules for the whole
session. Per-module `os.environ.setdefault(...)` stubs (as test_wasabi.py
carries) therefore only work when that module happens to be imported
first — e.g. tests/test_sync_missing_to_wasabi.py imports config via
scripts/sync_missing_to_wasabi.py with no stubs, which used to break
test_wasabi.py in full-suite runs.

conftest.py is imported by pytest before any test module, so stubbing
here guarantees every import order sees the same (test) values that the
modules see when run in isolation. setdefault keeps any values already
present in the real environment, and config.py's load_dotenv() does not
override existing os.environ entries.
"""

import os

os.environ.setdefault('API_KEY', 'test-key')
os.environ.setdefault('WASABI_ENDPOINT', 'https://s3.test.example')
os.environ.setdefault('WASABI_BUCKET', 's3://test-bucket/')
os.environ.setdefault('WASABI_PROFILE', 'test-profile')
