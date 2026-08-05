"""
run.py — LOCAL / DEV entrypoint only. Creates nothing itself: it imports the
shared Flask `app` and pulls in both feature modules so their routes get
registered on it, then runs migrations and starts Flask's built-in dev server.

Do NOT use this to serve the app on cPanel. Flask's dev server is
single-process and `debug=True` spawns an extra reloader process, doubling
your process count for no benefit. On cPanel, use "Setup Python App"
(Passenger/WSGI) instead, which imports `application` from passenger_wsgi.py.

Usage (local dev only): python run.py
"""
import os
from shared import app, run_migrations

# Importing these registers their @app.route()s on the shared app.
# (Imported for side effects — do not remove even though they look unused.)
import content_generation.app   # noqa: F401
import image_search.app         # noqa: F401
import worker                   # noqa: F401  # registers /api/worker/run


if __name__ == '__main__':
    run_migrations()
    debug_mode = os.getenv('FLASK_DEBUG', '0') == '1'
    app.run(debug=debug_mode, port=int(os.getenv('PORT', '5002')))