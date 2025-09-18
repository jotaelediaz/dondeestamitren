# app.py
from __future__ import annotations

import os
import sys

BASE_DIR = os.path.dirname(__file__)

venv_activate = os.path.join(BASE_DIR, "venv", "bin", "activate_this.py")
if os.path.exists(venv_activate):
    with open(venv_activate, encoding="utf-8") as f:
        code = compile(f.read(), venv_activate, "exec")
        exec(code, {"__file__": venv_activate})

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

os.environ.setdefault("PYTHONUNBUFFERED", "1")

from asgiref.wsgi import AsgiToWsgi  # noqa: E402

from app.main import app as fastapi_app  # noqa: E402

application = AsgiToWsgi(fastapi_app)
