# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DigitalDU Curation API — entry point.
"""

import logging
import threading

from flask import Flask
from flask_cors import CORS

import config
from lib import dcp, wasabi
from routes.qa import qa_bp
from routes.astools import astools_bp
from routes.aip import aip_bp
from routes.archive import archive_bp
from routes.convert import convert_bp

logger = logging.getLogger(__name__)


def create_app():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    config.validate_required()

    # Wasabi reachability probe. Logs the outcome but NEVER blocks startup:
    # the SFTP and local-fs endpoints work without Wasabi. Look for
    # `wasabi probe OK` in journald to confirm cred + bucket auth on boot.
    _probe_wasabi_at_startup()
    # DuraCloud Preserve probe: logged only, and run on a daemon thread so
    # an unreachable DCP can't delay any gunicorn worker's boot.
    threading.Thread(
        target=_probe_dcp_at_startup, name='dcp-startup-probe', daemon=True,
    ).start()

    app = Flask(__name__)
    CORS(app, resources={r'/api/*': {'origins': '*'}})

    app.register_blueprint(qa_bp)
    app.register_blueprint(astools_bp)
    app.register_blueprint(aip_bp)
    app.register_blueprint(archive_bp)
    app.register_blueprint(convert_bp)

    @app.route('/')
    def index():
        return config.APP_VERSION

    @app.route('/health')
    def health():
        return {'status': 'ok', 'version': config.APP_VERSION}, 200

    @app.route('/health/wasabi')
    def wasabi_health():
        """
        On-demand Wasabi probe. Distinct from the boot-time probe so
        ops can re-check connectivity after a config / network change
        without restarting the service. Returns 200 with the result
        regardless of probe outcome — the JSON body's `ok` field is
        the success signal.
        """
        result = wasabi.health_check()
        return result, 200

    @app.route('/health/dcp')
    def dcp_health():
        """
        On-demand DuraCloud Preserve probe: lists one key under each
        configured DCP location with the DCP credential. Same contract as
        /health/wasabi: always 200, and the JSON body's `ok` field is the
        success signal.
        """
        return dcp.health_check(), 200

    return app


def _probe_wasabi_at_startup():
    """
    Run the Wasabi health check once at startup and log the outcome.
    Wrapped in a try/except so even a bug in the probe itself can't
    abort the Flask app — the worst case should be a logged warning,
    never a non-booting service.
    """
    try:
        result = wasabi.health_check()
        if result['ok']:
            logger.info(
                'wasabi probe OK bucket=%s elapsed_ms=%d',
                result.get('bucket'), result.get('elapsed_ms', 0),
            )
        else:
            logger.warning(
                'wasabi probe FAILED bucket=%s err=%s elapsed_ms=%d',
                result.get('bucket'), result.get('error'),
                result.get('elapsed_ms', 0),
            )
    except Exception as e:  # noqa: BLE001
        # Belt-and-braces; health_check itself is supposed to catch
        # everything but we don't want a regression there to block boot.
        logger.warning('wasabi probe threw unexpectedly: %s', e)


def _probe_dcp_at_startup():
    """
    Log the DuraCloud Preserve probe once at startup. Skipped quietly when
    DCP isn't configured. Look for `dcp probe OK` in journald to confirm
    the DCP credential, region and locations on boot.
    """
    try:
        if not dcp.is_configured():
            logger.info('dcp probe skipped: not configured')
            return
        result = dcp.health_check()
        summary = ', '.join(
            '%s=%s' % (loc['name'], 'ok' if loc['ok'] else loc['error'])
            for loc in result['locations']
        ) or result.get('error')
        if result['ok']:
            logger.info(
                'dcp probe OK %s elapsed_ms=%d',
                summary, result.get('elapsed_ms', 0),
            )
        else:
            logger.warning(
                'dcp probe FAILED %s elapsed_ms=%d',
                summary, result.get('elapsed_ms', 0),
            )
    except Exception as e:  # noqa: BLE001
        # Same belt-and-braces as the Wasabi probe: never break boot.
        logger.warning('dcp probe threw unexpectedly: %s', e)


app = create_app()


if __name__ == '__main__':
    # Dev-only fallback. In production gunicorn imports `app:app`.
    app.run(host='0.0.0.0', port=int(config.APP_PORT))
