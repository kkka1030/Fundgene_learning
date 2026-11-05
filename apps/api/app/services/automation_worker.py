import asyncio
import logging
from collections.abc import Callable

from sqlalchemy.orm import Session

from app.core.config import Settings
from app.services.automations import AutomationSchedulerResult, run_due_automations

logger = logging.getLogger(__name__)


class AutomationBackgroundWorker:
    """Small in-process wrapper around the pure automation scheduler service."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="automation-worker")
        logger.info("automation background worker started")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info("automation background worker stopped")

    async def run_once(self) -> AutomationSchedulerResult:
        return await asyncio.to_thread(self._run_once_sync)

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("automation worker loop failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._settings.automation_worker_poll_seconds,
                )
            except asyncio.TimeoutError:
                continue

    def _run_once_sync(self) -> AutomationSchedulerResult:
        with self._session_factory() as db:
            result = run_due_automations(
                db,
                limit=self._settings.automation_worker_batch_size,
                retry_delay_minutes=self._settings.automation_worker_retry_delay_minutes,
            )
            if result.scanned_count:
                logger.info(
                    "automation worker scanned due settings",
                    extra={
                        "scanned": result.scanned_count,
                        "ran": result.ran_count,
                        "failed": result.failed_count,
                    },
                )
            return result
