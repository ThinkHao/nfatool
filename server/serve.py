from __future__ import annotations

import sys
from pathlib import Path

import uvicorn
from server.main import app
from server.config import get_settings

# Ensure package import works when running inside server/ directory
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

if __name__ == "__main__":
    # You can adjust host/port or enable reload via .env
    s = get_settings()
    uvicorn.run(app, host=s.HOST, port=int(s.PORT))
