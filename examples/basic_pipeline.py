"""
Basic Pipeline Example

This example demonstrates a simple end-to-end ETL pipeline with stagecraft:
- Loading data from a CSV file using CSVSource
- Transforming data in multiple stages
- Saving results using sproduce with a data source
- Running the pipeline with PipelineRunner

Run this example:
    python examples/basic_pipeline.py
"""

import logging
from typing import Annotated

import pandas as pd
import pandera.pandas as pa

from stagecraft import LoggingManagerConfig, setup_logger
from stagecraft.core.dataclass import autodataclass
from stagecraft.pipeline.data_source import CSVSource
from stagecraft.pipeline.definition import PipelineDefinition
from stagecraft.pipeline.runner import PipelineRunner
from stagecraft.pipeline.schemas import DFVarSchema
from stagecraft.pipeline.stages import ETLStage
from stagecraft.pipeline.variables import DFVar

setup_logger(
    LoggingManagerConfig(
        level=logging.DEBUG,
    )
)

logger = logging.getLogger(__name__)


@autodataclass
class RawDataSchema(DFVarSchema, index_cols=["date", "product"]):
    """Schema for raw sales data."""

    date: pd.Timestamp
    product: Annotated[str, pa.Field(str_length={"max_value": 100})]
    quantity: Annotated[int, pa.Field(ge=0)]
    price: Annotated[float, pa.Field(ge=0.0)]


@autodataclass
class ProcessedDataSchema(RawDataSchema):
    """Schema for processed sales data with total column."""

    total: Annotated[float, pa.Field(ge=0.0)]


class TransformDataStage(ETLStage):
    """Loads raw sales data and transforms it by calculating totals and filtering. Saves the processed data."""

    raw_data = DFVar(
        RawDataSchema,
        description="Raw sales data from CSV",
        source=CSVSource("examples/data/sales.csv", mode="r"),
    ).sconsume()

    processed_data = DFVar(
        ProcessedDataSchema,
        description="Processed sales data with totals",
        source=CSVSource("examples/output/processed_sales.csv", mode="w"),
    ).sproduce()

    def recipe(self):
        df = self.raw_data.copy()
        df["total"] = df["quantity"] * df["price"]
        df = df[df["total"] > 0]
        df["date"] = pd.to_datetime(df["date"])
        logger.debug(f"Processed {len(df)} rows")
        logger.debug(f"Total revenue: ${df['total'].sum():.2f}")
        self.processed_data = df


def main():
    """Run the basic pipeline."""
    pipeline = PipelineDefinition(
        name="basic_sales_pipeline",
        stages=[TransformDataStage()],
    )

    runner = PipelineRunner()
    result = runner.run(pipeline)

    if result.success:
        if result.metadata:
            logger.debug(f"Duration: {result.metadata.duration:.2f} seconds")
            logger.debug(f"Stages executed: {len(result.metadata.stages_executed)}")
    else:
        logger.error(f"Error: {result.error}")


if __name__ == "__main__":
    main()
    pass
