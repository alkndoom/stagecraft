import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

from ..core.logging import ANSIColors
from .conditions import StageCondition
from .context import PipelineContext
from .helpers import SValuable
from .pipeline_metadata import StageParameter
from .stages import ETLStage

logger = logging.getLogger("loop")


class StageLoop(ETLStage):
    """
    A composite stage that executes a group of stages in a loop.

    The loop continues while a condition variable in the context is True.
    Useful for pagination, iterative processing, or retry logic.

    Example:
        >>> pipeline = PipelineDefinition(
        ...     name="paginated_scraper",
        ...     stages=[
        ...         NavigateStage(),
        ...         StageLoop(
        ...             stages=[
        ...                 DiscoverItemsStage(),
        ...                 CheckLoadMoreStage(),
        ...                 ClickLoadMoreStage(),
        ...             ],
        ...             condition="has_more_pages",
        ...             max_iterations=50,
        ...         ),
        ...         ProcessAllItemsStage(),
        ...     ]
        ... )
    """

    def __init__(
        self,
        stages: List[ETLStage],
        condition: Union[str, SValuable[StageCondition], SValuable[bool]],
        max_iterations: Optional[int] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        context: Optional[PipelineContext] = None,
        parameters: Optional[List[StageParameter]] = None,
    ):
        """
        Initialize a looping stage group.

        Args:
            stages: List of stages to execute in each loop iteration
            condition: Either a variable name (str) to check for truthiness,
                      or a StageCondition instance for complex logic
            max_iterations: Maximum number of loop iterations (safety limit)
            name: Optional custom name for the loop stage
        """
        assert stages, "At least one stage must be provided for StageLoop"
        assert condition, "Condition must be provided for StageLoop"
        assert max_iterations is None or max_iterations > 0, "max_iterations must be positive"

        self.loop_stages = stages

        super().__init__(
            name=name or f"loop_{ '_'.join(s.name for s in stages[:2]) }_etc",
            description=description or f"Loop executing {len(stages)} stages",
            context=context,
            parameters=parameters,
            condition=condition,
        )

        # Track loop metadata
        self.mean_loop_time: Dict[str, float] = {}
        self.mean_iteration_time: float = 0.0
        self.max_iterations = max_iterations
        self.current_iteration = 0
        self.total_iterations = 0

    def collect_all_stages(self) -> List[ETLStage]:
        stages = super().collect_all_stages()
        for loop_stage in getattr(self, "loop_stages", []):
            stages.extend(loop_stage.all_stages)
        return stages

    def _should_continue_loop(self) -> bool:
        """Check if the loop should continue based on condition."""
        if self.context is None:
            raise RuntimeError("PipelineContext is not set for StageLoop")

        self._resolve_condition()
        return self.condition.should_execute(self.context, self.name)

    def recipe(self):
        """Execute the loop of stages."""
        self.current_iteration = 0
        loop_times: Dict[str, List[float]] = {s.name: [] for s in self.loop_stages}
        iteration_times: List[float] = []

        while (
            (self.current_iteration < self.max_iterations)
            if self.max_iterations is not None
            else True
        ):
            if not self._should_continue_loop():
                break
            iteration_start = datetime.now()

            self.current_iteration += 1
            logger.info(
                f"Starting iteration color_fmt([{self.current_iteration}], {ANSIColors.DEFAULT}) of StageLoop '{self.name}'"
            )
            for stage in self.loop_stages:
                stage.execute()
                loop_time = (stage.end_time - stage.start_time).microseconds / 1000  # type: ignore
                loop_times[stage.name].append(loop_time)

            iteration_time = (datetime.now() - iteration_start).microseconds / 1000  # type: ignore
            iteration_times.append(iteration_time)
        self.total_iterations = self.current_iteration
        self.mean_loop_time = {
            name: round(sum(t) / len(t), 3) if t else 0.0 for name, t in loop_times.items()
        }
        self.mean_iteration_time = (
            round(sum(iteration_times) / len(iteration_times), 3) if iteration_times else 0.0
        )
        self.execution_metadata["total_iterations"] = self.total_iterations
        self.execution_metadata["mean_iteration_time"] = self.mean_iteration_time
        self.execution_metadata["mean_loop_time"] = self.mean_loop_time
