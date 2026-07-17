"""
passenger_wsgi.py — the entrypoint cPanel's "Setup Python App" (Passenger)
looks for. Passenger imports this file and expects a module-level name
called `application` implementing WSGI — it does NOT run run.py or call
app.run() itself, so debug mode / the Flask dev server never come into play
in production.

Passenger also manages its own process count for you: in cPanel's
"Setup Python App" UI you set the number of application processes there
(keep this LOW on shared hosting — 1-2 — since each process loads the full
app, DB pool, etc.). Don't also try to run multiple copies via run.py.
"""
from shared import app, run_migrations

# Importing these registers their @app.route()s on the shared app.
import content_generation.app   # noqa: F401
import image_search.app         # noqa: F401

# Run migrations once when Passenger loads this process (safe/idempotent —
# see run_migrations() in shared.py).
run_migrations()

application = app
