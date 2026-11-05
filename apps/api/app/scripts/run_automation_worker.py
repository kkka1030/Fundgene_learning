import argparse
import logging
import time

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.services.automations import run_due_automations


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FundGene automation scheduler.")
    parser.add_argument("--once", action="store_true", help="Run one scheduler pass.")
    parser.add_argument("--limit", type=int, default=None, help="Due settings per pass.")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=None,
        help="Loop polling interval when --once is omitted.",
    )
    return parser.parse_args()


def _run_once(*, limit: int, retry_delay_minutes: int) -> None:
    with SessionLocal() as db:
        result = run_due_automations(
            db,
            limit=limit,
            retry_delay_minutes=retry_delay_minutes,
        )
    print(
        "automation scheduler pass: "
        f"scanned={result.scanned_count} "
        f"ran={result.ran_count} "
        f"failed={result.failed_count} "
        f"proposals={len(result.created_pending_proposal_ids)} "
        f"notifications={len(result.created_notification_ids)}"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    settings = get_settings()
    limit = args.limit or settings.automation_worker_batch_size
    poll_seconds = args.poll_seconds or settings.automation_worker_poll_seconds
    retry_delay_minutes = settings.automation_worker_retry_delay_minutes

    if args.once:
        _run_once(limit=limit, retry_delay_minutes=retry_delay_minutes)
        return

    while True:
        _run_once(limit=limit, retry_delay_minutes=retry_delay_minutes)
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
