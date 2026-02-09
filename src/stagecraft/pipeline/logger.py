from __future__ import annotations

import logging
from typing import Any, Dict, Optional


class StdPipelineLogger:
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def pipeline_started(
        self,
        *,
        pipeline: str,
        run_id: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.info(
            "pipeline_started",
            extra={"pipeline": pipeline, "run_id": run_id, **(extra or {})},
        )

    def pipeline_completed(
        self,
        *,
        pipeline: str,
        run_id: str,
        status: str,
        duration_sec: float,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.info(
            "pipeline_completed",
            extra={
                "pipeline": pipeline,
                "run_id": run_id,
                "status": status,
                "duration_sec": duration_sec,
                **(extra or {}),
            },
        )

    def stage_started(
        self,
        *,
        pipeline: str,
        stage: str,
        run_id: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.info(
            "stage_started",
            extra={
                "pipeline": pipeline,
                "stage": stage,
                "run_id": run_id,
                **(extra or {}),
            },
        )

    def stage_completed(
        self,
        *,
        pipeline: str,
        stage: str,
        run_id: str,
        status: str,
        duration_sec: float,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.info(
            "stage_completed",
            extra={
                "pipeline": pipeline,
                "stage": stage,
                "run_id": run_id,
                "status": status,
                "duration_sec": duration_sec,
                **(extra or {}),
            },
        )

    def warning(
        self,
        *,
        event: str,
        run_id: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.warning(
            event,
            extra={"run_id": run_id, **(extra or {})},
        )

    def error(
        self,
        *,
        event: str,
        run_id: str,
        error_type: str,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.error(
            event,
            extra={
                "run_id": run_id,
                "error_type": error_type,
                "message": message,
                **(extra or {}),
            },
        )

    def debug(
        self,
        *,
        event: str,
        run_id: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._logger.debug(
            event,
            extra={"run_id": run_id, **(extra or {})},
        )
