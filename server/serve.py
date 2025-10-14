from __future__ import annotations

import sys
from pathlib import Path

# Ensure package import works when running inside server/ directory
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

import uvicorn
from server.main import app
from server.config import get_settings

if __name__ == "__main__":
    # You can adjust host/port or enable reload via .env
    s = get_settings()
    import signal
    config = uvicorn.Config(app, host=s.HOST, port=int(s.PORT), lifespan="on")
    server = uvicorn.Server(config)

    def _handle_exit(signum, frame):
        try:
            server.should_exit = True
        except Exception:
            pass

    try:
        signal.signal(signal.SIGINT, _handle_exit)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, _handle_exit)
    except Exception:
        pass

    try:
        server.run()
    except KeyboardInterrupt:
        pass
