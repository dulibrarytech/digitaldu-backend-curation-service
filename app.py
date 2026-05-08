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

from flask import Flask
from flask_cors import CORS

import config
from routes.qa import qa_bp
from routes.astools import astools_bp


def create_app():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    config.validate_required()

    app = Flask(__name__)
    CORS(app, resources={r'/api/*': {'origins': '*'}})

    app.register_blueprint(qa_bp)
    app.register_blueprint(astools_bp)

    @app.route('/')
    def index():
        return config.APP_VERSION

    @app.route('/health')
    def health():
        return {'status': 'ok', 'version': config.APP_VERSION}, 200

    return app


app = create_app()


if __name__ == '__main__':
    # Dev-only fallback. In production gunicorn imports `app:app`.
    app.run(host='0.0.0.0', port=int(config.APP_PORT))
