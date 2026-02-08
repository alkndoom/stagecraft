import logging
from datetime import datetime
from typing import Any, Dict, Optional, Set, Union

from .context import PipelineContext
from .definition import PipelineDefinition, invert_dependency_map
from .memory import MemoryConfig
from .pipeline_metadata import (
    ExecutionStatus,
    PipelineExecutionMetadata,
    PipelineResult,
    StageExecutionMetadata,
)
from .stages import ETLStage

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, memory_config: Optional[MemoryConfig] = None):
        self.current_pipeline: Optional[PipelineDefinition] = None
        self.execution_metadata: Optional[PipelineExecutionMetadata] = None
        self.memory_config = memory_config or MemoryConfig()

    def __initialize_execution(self, pipeline: PipelineDefinition) -> None:
        """Initialize pipeline execution."""
        self.current_pipeline = pipeline
        self.execution_metadata = PipelineExecutionMetadata(
            pipeline_name=pipeline.name,
            start_time=datetime.now(),
            status=ExecutionStatus.RUNNING,
        )

        logger.info(f"Starting pipeline execution: {pipeline.name}")
        # logger.info(f"Pipeline metadata: {pipeline.get_metadata()}")

    def __finalize_execution(self, success: bool, error: Exception = None) -> None:  # type: ignore
        """Finalize pipeline execution."""
        if self.execution_metadata:
            self.execution_metadata.end_time = datetime.now()
            self.execution_metadata.duration = (
                self.execution_metadata.end_time - self.execution_metadata.start_time
            ).total_seconds()

            if success:
                self.execution_metadata.status = ExecutionStatus.COMPLETED
                logger.info(
                    f"Pipeline completed successfully in {self.execution_metadata.duration:.2f} seconds"
                )
            else:
                self.execution_metadata.status = ExecutionStatus.FAILED
                self.execution_metadata.error = error
                logger.warning(
                    f"Pipeline failed after {self.execution_metadata.duration:.2f} seconds"
                )

    def run(
        self,
        pipeline: PipelineDefinition,
        context: Optional[Union[Dict[str, Any], PipelineContext]] = None,
    ) -> PipelineResult:
        """
        Execute a complete ETL pipeline.

        Args:
            pipeline: Pipeline definition to execute

        Returns:
            Dictionary containing execution results and metadata
        """
        try:
            if isinstance(context, dict):
                context = PipelineContext(context, memory_config=self.memory_config)
            elif context is None:
                context = PipelineContext(memory_config=self.memory_config)
            else:
                self.memory_config = context.memory_manager.config

            pipeline.validate(context.variables)
            self.__initialize_execution(pipeline)

            # Inject context into all stages
            for stage in pipeline.stages:
                stage.set_context(context)

            self.__execute_pipeline_stages(pipeline, context)

            # Log final memory summary
            if self.memory_config.enabled and self.memory_config.log_memory_usage:
                context.log_memory_summary()

            self.__finalize_execution(True)
            result = PipelineResult(success=True, metadata=self.execution_metadata)
            return result
        except Exception as e:
            self.__finalize_execution(False, e)
            result = PipelineResult(success=False, metadata=self.execution_metadata, error=e)
            return result

    def __record_stage_metadata(self, metadata: StageExecutionMetadata) -> None:
        """Record stage execution metadata to pipeline execution metadata."""
        if self.execution_metadata:
            self.execution_metadata.stages_executed.append(metadata)

    def __create_stage_execution_metadata(
        self,
        stage: ETLStage,
        status: ExecutionStatus,
        error: Optional[Exception] = None,
    ) -> StageExecutionMetadata:
        """Create stage execution metadata with calculated duration."""
        try:
            duration = (stage.end_time - stage.start_time).total_seconds()  # type: ignore
        except Exception:
            duration = 0.0
        return StageExecutionMetadata(
            name=stage.name,
            duration=duration,
            status=status,
            error=error,
            sub_stages=[
                self.__create_stage_execution_metadata(s, status) for s in stage.sub_stages
            ],
            additional_info=stage.execution_metadata,
        )

    def __handle_skipped_stage(self, stage: ETLStage, completed_stages: Set[str]) -> None:
        """Handle a stage that was skipped due to its execution condition.

        When a stage is skipped, all its sub-stages are also marked as completed
        for memory management purposes, since they won't be executed either.
        """
        skip_reason = stage.get_skip_reason()
        metadata = self.__create_stage_execution_metadata(stage, ExecutionStatus.SKIPPED)
        self.__record_stage_metadata(metadata)
        logger.info(f"Skipping stage '{stage.name}': {skip_reason}")
        for s in stage.all_stages:
            completed_stages.add(s.name)

    def __handle_successful_stage(self, stage: ETLStage, completed_stages: Set[str]) -> None:
        """Handle a stage that executed successfully."""
        metadata = self.__create_stage_execution_metadata(stage, ExecutionStatus.COMPLETED)
        self.__record_stage_metadata(metadata)
        logger.info(f"Stage {stage.name} completed in {metadata.duration:.2f} seconds")
        for s in stage.all_stages:
            completed_stages.add(s.name)

    def __handle_failed_stage(self, stage: ETLStage, error: Exception) -> None:
        """Handle a stage that failed during execution."""
        metadata = self.__create_stage_execution_metadata(stage, ExecutionStatus.FAILED, error)
        self.__record_stage_metadata(metadata)
        logger.error(f"Stage '{stage.name}' failed: {error}")

    def __auto_clear_memory_if_enabled(
        self,
        context: PipelineContext,
        inverted_dependency_map: Dict[str, Set[str]],
        completed_stages: Set[str],
    ) -> None:
        """Auto-clear unused variables from memory if memory management is enabled."""
        cleared = context.auto_clear_unused_variables(inverted_dependency_map, completed_stages)
        if len(cleared) > 0:
            logger.info(f"Auto-cleared {len(cleared)} unused variable(s) from memory: {cleared}")

    def __execute_single_stage(
        self,
        stage: ETLStage,
        context: PipelineContext,
        inverted_dependency_map: Dict[str, Set[str]],
        completed_stages: Set[str],
    ) -> None:
        """Execute a single pipeline stage with error handling and metadata tracking.

        When a stage executes successfully, this method marks both the stage and all
        its sub-stages as completed. This ensures that memory management can properly
        clear variables that are only consumed by sub-stages.
        """
        try:
            stage_result = stage.execute()
            if not stage_result:
                self.__handle_skipped_stage(stage, completed_stages)
                return

            self.__handle_successful_stage(stage, completed_stages)
            self.__auto_clear_memory_if_enabled(context, inverted_dependency_map, completed_stages)
        except Exception as e:
            self.__handle_failed_stage(stage, e)
            raise

    def __execute_pipeline_stages(self, pipeline: PipelineDefinition, context: PipelineContext):
        """Execute pipeline stages sequentially."""
        completed_stages: Set[str] = set()

        for stage in pipeline.stages:
            self.__execute_single_stage(
                stage,
                context,
                invert_dependency_map(pipeline.dependency_map),
                completed_stages,
            )

    def get_execution_summary(self) -> Optional[PipelineExecutionMetadata]:
        """Get summary of the last pipeline execution."""
        return self.execution_metadata
