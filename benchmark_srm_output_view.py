import statistics
import time
from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd

from olake.lakehouse import ObservationLakehouse, ObservationAnalyzer


@dataclass
class QueryBenchmark:
    problem_id: str
    query_type: str
    impls: int
    tests: int
    execution_time_ms: float
    timestamp: datetime

lakehouse = ObservationLakehouse(
    warehouse_path="./warehouse",
    catalog_db_path="iceberg_catalog.db"
)

analyzer = ObservationAnalyzer(lakehouse)

iceberg_path = lakehouse.load_observations_table().metadata_location

# fetch all problems
all_problems = analyzer.conn.execute(f"""
    SELECT data_set_id, problem_id FROM iceberg_scan('{iceberg_path}')
    GROUP BY data_set_id, problem_id
""").fetchdf()

print(all_problems)

# repetitions per problem
runs = 10
# collect data points
benchmarks = []

for index, row in all_problems.iterrows():
    print(f"{row['data_set_id']} -> {row['problem_id']}")

    data_set_id = row['data_set_id']
    problem_id = row['problem_id']

    avg = []
    for i in range(0, runs):
        behave_query = f"""-- CTE to fetch outputs.
    WITH test_sequences AS (
        SELECT
            --run_id,
            --problem_id,
            test_id,
            step_id,
            implementation_id,
            --operation,
            --inputs,
            output
        FROM
            iceberg_scan('{iceberg_path}')
        WHERE
            problem_id = '{problem_id}'
        ORDER BY
            test_id, step_id
    )
    -- Now, pivot the prepared data to create SRM output view
    PIVOT test_sequences
    ON implementation_id
    USING FIRST(output)
    -- GROUP BY: Specifies the column that will serve as the row identifier for the matrix.
    GROUP BY
        test_id, step_id
    ORDER BY
        test_id, step_id
    """

        start_time = time.perf_counter()

        result = analyzer.conn.execute(behave_query).fetchdf()

        end_time = time.perf_counter()

        execution_time_ms = (end_time - start_time) * 1000

        print(f"time msecs = {execution_time_ms}")
        #print(result.to_latex())
        impls = result.shape[1] - 2 # minus two columns (test_id, step_id)
        tests = result.shape[0]
        comparisons = tests * (impls * (impls - 1) / 2)
        print(f"impls = {impls} tests = {tests}. Comparisons = {comparisons}")

        avg.append(execution_time_ms)

        benchmarks.append(QueryBenchmark(
            problem_id=problem_id,
            query_type='srm output view',
            impls=impls,
            tests=tests,
            execution_time_ms=execution_time_ms,
            timestamp=datetime.now()
        ))

    print(f"mean = {statistics.mean(avg)}")


df = pd.DataFrame([asdict(b) for b in benchmarks])
df.to_csv("benchmark_srm_view.csv")

print(df.describe())