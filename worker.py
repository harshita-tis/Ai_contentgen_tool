"""
worker.py — runs generation jobs OUTSIDE the web request process.

Why this exists
----------------
On cPanel, the web app runs under Passenger, which can recycle/kill its
worker process at any time (memory limit, idle timeout, max requests hit).
When the actual product-content generation ran as a background thread
*inside* that same Passenger process, killing the process killed the thread
mid-job too — that's the "Child process ... killed by signal: 15" you saw.
It worked locally only because `python run.py` is a single long-lived
process that nothing ever recycles.

The fix: `/api/generate-stream` now only writes a 'pending' GenerationJob
row (with its full payload) to the database and returns immediately. This
script is a completely separate process that polls for pending jobs and
actually runs them, using the same `_run_generation_job()` used before —
unchanged, since it already reads/writes all of its state through the DB.

How to run it
--------------
Locally (dev): keep this running in a second terminal alongside `run.py`:
    python worker.py --loop

On cPanel: you generally cannot keep an arbitrary script running forever,
so instead schedule it as a cron job (cPanel -> Cron Jobs) to run every
minute in "drain" mode, which processes whatever is pending and exits:
    * * * * * cd /home/USER/your_app && /home/USER/virtualenv/.../bin/python worker.py --once >> worker.log 2>&1

Because each cron invocation is a fresh, independent process, Passenger
recycling the *web* process has no effect on it -- and if the worker process
itself gets killed mid-job, the job just sits at status='running' until the
next run reclaims it (see _reclaim_stale_jobs) and retries it from scratch.
"""
import argparse
import json
import os
import time
import logging

from flask import request, jsonify
from shared import app, db, GenerationJob

logger = logging.getLogger(__name__)

# Shared secret required to trigger the worker over HTTP (see /api/worker/run
# below). Set this in your .env / cPanel "Setup Python App" env vars to a
# long random string, and use the SAME value in the cron URL's ?secret=...
# Without a real secret set, the endpoint refuses every request.
WORKER_SECRET = os.getenv('WORKER_SECRET', '')

# How many jobs this worker process will run at the same time. Keep this LOW
# on shared/cPanel hosting for the same reasons described in content_generation
# -- each job itself opens GEN_SECTION_WORKERS threads to call OpenAI.
MAX_CONCURRENT_JOBS = int(os.getenv('MAX_CONCURRENT_JOBS', '2'))

# If a job has been sitting at status='running' with no update for longer
# than this, assume the worker process that owned it died (killed, crashed,
# server restarted) and put it back in the queue so another run picks it up.
STALE_RUNNING_SECONDS = int(os.getenv('STALE_JOB_SECONDS', '900'))  # 15 min

# In --once mode, stop picking up new jobs after this many seconds so a
# cron-scheduled run doesn't run forever / overlap the next cron tick.
# Only matters for --once; --loop ignores this and runs until killed.
DEFAULT_TIME_BUDGET_SECONDS = int(os.getenv('WORKER_TIME_BUDGET', '50'))


def _reclaim_stale_jobs():
    """Reset jobs stuck in 'running' (owning process died) back to 'pending'."""
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=STALE_RUNNING_SECONDS)
    with app.app_context():
        stale = GenerationJob.query.filter(
            GenerationJob.status == 'running',
            GenerationJob.updated_at < cutoff,
        ).all()
        for job in stale:
            logger.warning(f"Reclaiming stale job {job.job_id} (no update since {job.updated_at})")
            job.status = 'pending'
        if stale:
            db.session.commit()


def _next_pending_job_id():
    with app.app_context():
        job = (GenerationJob.query
               .filter_by(status='pending')
               .order_by(GenerationJob.created_at.asc())
               .first())
        return job.job_id if job else None


def _run_one(job_id: str):
    """Load a job's persisted payload and run it via the existing pipeline."""
    # Imported lazily so `worker.py` can be imported/tested without pulling in
    # the OpenAI client etc. unless actually running a job.
    from content_generation.app import _run_generation_job

    with app.app_context():
        job = GenerationJob.query.filter_by(job_id=job_id).first()
        if not job or job.status != 'running':
            return  # someone else picked it up already, or it was cancelled
        try:
            payload = json.loads(job.payload or '{}')
        except Exception:
            job.status = 'error'
            job.error_message = 'Corrupt job payload'
            db.session.commit()
            return
        batch_id = job.batch_id

    logger.info(f"[worker] starting job {job_id}")
    try:
        _run_generation_job(
            job_id, batch_id,
            payload.get('products', []),
            payload.get('active_sections', []),
            payload.get('section_prompts', {}),
            payload.get('section_templates', {}),
            cancel_event=None,   # no in-memory event across processes -- DB status is authoritative
        )
    except Exception:
        logger.exception(f"[worker] job {job_id} raised unexpectedly")
        with app.app_context():
            job = GenerationJob.query.filter_by(job_id=job_id).first()
            if job and job.status not in ('done', 'cancelled'):
                job.status = 'error'
                job.error_message = 'Worker crashed while processing this job.'
                db.session.commit()
    logger.info(f"[worker] finished job {job_id}")


def drain(time_budget_seconds):
    """Process pending jobs until the queue is empty or the time budget runs out.

    time_budget_seconds=None means run forever (--loop mode).
    """
    from concurrent.futures import ThreadPoolExecutor

    start = time.monotonic()
    _reclaim_stale_jobs()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix='worker-job') as pool:
        in_flight = {}
        while True:
            if time_budget_seconds is not None and (time.monotonic() - start) > time_budget_seconds:
                break

            # Top up the pool with new pending jobs
            while len(in_flight) < MAX_CONCURRENT_JOBS:
                job_id = _next_pending_job_id()
                if not job_id or job_id in in_flight:
                    break
                with app.app_context():
                    j = GenerationJob.query.filter_by(job_id=job_id).first()
                    if j and j.status == 'pending':
                        j.status = 'running'
                        db.session.commit()
                    else:
                        continue
                in_flight[job_id] = pool.submit(_run_one, job_id)

            if not in_flight:
                if time_budget_seconds is None:
                    time.sleep(3)   # --loop mode: idle-poll
                    continue
                else:
                    break            # --once mode: nothing left to do, exit

            done_ids = [jid for jid, fut in in_flight.items() if fut.done()]
            for jid in done_ids:
                in_flight.pop(jid)
            if not done_ids:
                time.sleep(1)

        # --once mode: don't abandon in-flight jobs, wait for them before exiting
        for fut in in_flight.values():
            fut.result()


@app.route('/api/worker/run', methods=['GET', 'POST'])
def worker_http_trigger():
    """
    URL-triggered equivalent of `python worker.py --once`, for hosts where
    cron can only hit a URL (cPanel's own cron using wget/curl, or an
    external cron service like cron-job.org).

    Call it as:
        https://yourdomain.com/api/worker/run?secret=YOUR_WORKER_SECRET

    Runs synchronously and returns once the drain finishes or the time
    budget (default DEFAULT_TIME_BUDGET_SECONDS, override with
    ?time_budget=30) runs out -- same semantics as --once.

    Caveat vs. the cron/`--once` approach: this executes inside the same
    Passenger-managed web process as the rest of the app, so if Passenger
    recycles that process mid-drain, any job still 'running' gets orphaned
    until the next call reclaims it (_reclaim_stale_jobs) -- it does NOT
    get the full process isolation a separate cron-run script has.
    """
    if not WORKER_SECRET or request.args.get('secret') != WORKER_SECRET:
        return jsonify({'error': 'unauthorized'}), 403

    time_budget = request.args.get('time_budget', DEFAULT_TIME_BUDGET_SECONDS, type=int)
    drain(time_budget_seconds=time_budget)
    return jsonify({'status': 'ok', 'time_budget': time_budget}), 200


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Runs pending generation jobs.')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once', action='store_true',
                       help='Drain the queue once (with a time budget) and exit. Use this from cron on cPanel.')
    mode.add_argument('--loop', action='store_true',
                       help='Run forever, polling for new jobs. Use this for local dev / a real VPS.')
    parser.add_argument('--time-budget', type=int, default=DEFAULT_TIME_BUDGET_SECONDS,
                         help='Seconds to keep picking up new jobs before exiting, in --once mode.')
    args = parser.parse_args()

    if args.once:
        drain(time_budget_seconds=args.time_budget)
    else:
        drain(time_budget_seconds=None)