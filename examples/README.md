# Stagecraft Examples

This directory contains practical examples demonstrating common usage patterns for the stagecraft ETL pipeline library.

## Examples Overview

### 1. Basic Pipeline (`basic_pipeline.py`)

A simple end-to-end pipeline demonstrating the fundamental concepts of stagecraft.

**What it demonstrates:**
- Loading data from CSV files using `CSVSource`
- Creating pipeline stages with `ETLStage`
- Transforming data in stages using `sconsume` and `sproduce`
- Saving results to CSV files
- Running pipelines with `PipelineRunner`

**How to run:**
```bash
python examples/basic_pipeline.py
```

**Expected output:**
- Loads 10 rows of sales data
- Calculates total revenue for each sale
- Filters out invalid records
- Saves processed data to `examples/output/processed_sales.csv`
- Displays summary statistics

**Key concepts:**
- `CSVSource` for reading and writing CSV files (note: uses `;` separator by default)
- `DFVar` for DataFrame variables
- `sconsume()` to declare input variables
- `sproduce()` to declare output variables
- Stage `recipe()` method for transformation logic

---

### 2. DataFrame Pipeline with Pandera Schema (`dataframe_pipeline.py`)

Demonstrates type-safe DataFrame operations with Pandera schema validation.

**What it demonstrates:**
- Defining custom Pandera `DataFrameModel` schemas
- Using `DFVar` with schema validation
- Automatic type coercion and validation
- Schema-driven data quality checks
- Enriching data with derived fields

**How to run:**
```bash
python examples/dataframe_pipeline.py
```

**Expected output:**
- Loads and validates 10 customer records
- Enriches data with age groups and account age
- Validates output against enriched schema
- Saves enriched data to `examples/output/enriched_customers.csv`
- Displays validation results and statistics

**Key concepts:**
- `pa.DataFrameModel` for defining schemas
- `pa.Field()` for column validation rules (ranges, constraints)
- Schema validation with automatic type coercion
- `DFVar(SchemaClass)` for type-safe DataFrames
- Validation happens automatically on load and save

**Schema features shown:**
- Integer constraints (`ge=1`, `le=120`)
- String validation (`isin=["Young Adult", "Adult", "Senior"]`)
- Timestamp handling
- Automatic type coercion (`coerce=True`)

---

### 3. Conditional Execution (`conditional_execution.py`)

Shows how to control stage execution using various condition types.

**What it demonstrates:**
- `ConfigFlagCondition` - Execute based on configuration flags
- `VariableExistsCondition` - Execute only if a variable exists
- `InputNotEmptyCondition` - Execute only if input is not empty
- `AndCondition` - Execute only if ALL conditions are met
- `OrCondition` - Execute if ANY condition is met
- `CustomCondition` - Define custom execution logic

**How to run:**
```bash
python examples/conditional_execution.py
```

**Expected output:**
- Demonstrates each condition type in action
- Shows which stages execute and which are skipped
- Displays skip reasons for skipped stages
- Summary of execution status for all stages

**Key concepts:**
- Stage `condition` attribute controls execution
- Conditions are evaluated before stage execution
- Skipped stages don't break the pipeline
- Combine conditions with `AndCondition` and `OrCondition`
- Custom logic with `CustomCondition`

**Condition types:**
```python
# Config-based
condition = ConfigFlagCondition("flag_name", {"flag_name": True})

# Variable existence
condition = VariableExistsCondition("variable_name")

# Input validation
condition = InputNotEmptyCondition("input_variable")

# Combine conditions
condition = AndCondition([condition1, condition2])
condition = OrCondition([condition1, condition2])

# Custom logic
def my_check(context, stage_name):
    return True  # Your logic here

condition = CustomCondition(my_check, skip_reason="Custom reason")
```

---

## Data Files

### `data/sales.csv`
Sample sales data with columns:
- `date` - Transaction date
- `product` - Product name
- `quantity` - Quantity sold
- `price` - Unit price

### `data/customers.csv`
Sample customer data with columns:
- `customer_id` - Unique customer identifier
- `name` - Customer name
- `email` - Email address
- `age` - Customer age
- `signup_date` - Account creation date

**Important:** CSV files use semicolon (`;`) as the separator, which is the default for stagecraft's `CSVSource`.

---

## Output Files

When you run the examples, output files will be created in `examples/output/`:

- `processed_sales.csv` - Processed sales data with calculated totals
- `enriched_customers.csv` - Customer data with age groups and account age

---

## Common Patterns

### Loading Data from CSV
```python
class LoadStage(ETLStage):
    data = sproduce(
        DFVar(
            source=CSVSource("path/to/file.csv", mode="r"),
            description="Data description"
        )
    )
    
    def recipe(self):
        # Data is automatically loaded
        print(f"Loaded {len(self.data)} rows")
```

### Transforming Data
```python
class TransformStage(ETLStage):
    input_data = sconsume(DFVar())
    output_data = sproduce(DFVar())
    
    def recipe(self):
        df = self.input_data.copy()
        # Transform df
        self.output_data = df
```

### Saving Data to CSV
```python
class SaveStage(ETLStage):
    data = sconsume(
        DFVar(
            source=CSVSource("output/file.csv", mode="w"),
            description="Output data"
        )
    )
    
    def recipe(self):
        # Data is automatically saved
        pass
```

### Running a Pipeline
```python
pipeline = PipelineDefinition(
    name="my_pipeline",
    stages=[Stage1(), Stage2(), Stage3()]
)

runner = PipelineRunner()
result = runner.run(pipeline)

if result.success:
    print("Success!")
else:
    print(f"Failed: {result.error}")
```

---

## Tips and Best Practices

1. **CSV Separator**: Stagecraft uses `;` as the default CSV separator. If your files use `,`, you'll need to handle this in your data preparation.

2. **Schema Validation**: Always define Pandera schemas for DataFrames to catch data quality issues early.

3. **Error Handling**: The pipeline runner catches exceptions and provides detailed error information in `result.error`.

4. **Memory Management**: For large datasets, consider using `MemoryConfig` with `PipelineRunner(memory_config=config)`.

5. **Conditional Execution**: Use conditions to make pipelines more flexible and skip unnecessary processing.

6. **Stage Naming**: Give stages descriptive names for better logging and debugging.

7. **Data Sources**: Use `mode="r"` for read-only, `mode="w"` for write-only, and `mode="w+"` for read-write access.

---

## Next Steps

- Explore the main [README.md](../README.md) for complete API documentation
- Check out the [CHANGELOG.md](../CHANGELOG.md) for version history
- Review the source code in `src/stagecraft/` for advanced usage

---

## Troubleshooting

**Issue**: CSV file not found
- **Solution**: Make sure you're running examples from the project root directory

**Issue**: Schema validation fails
- **Solution**: Check that your data matches the schema definition, especially data types and constraints

**Issue**: Stage is skipped unexpectedly
- **Solution**: Check the stage's condition and verify that required variables exist in the pipeline context

**Issue**: Import errors
- **Solution**: Make sure stagecraft is installed: `pip install stagecraft` or `pip install -e .` for development

