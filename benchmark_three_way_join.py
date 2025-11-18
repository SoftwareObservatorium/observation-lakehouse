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

observations_table = lakehouse.load_observations_table()
code_implementations_table = lakehouse.load_code_table()
tests_table = lakehouse.load_test_table()

# fetch all problems
all_problems = analyzer.conn.execute(f"""
    SELECT data_set_id, problem_id FROM iceberg_scan('{observations_table.metadata_location}')
    GROUP BY data_set_id, problem_id
""").fetchdf()

print(all_problems)

# repetitions per problem
runs = 1
# collect data points
benchmarks = []

for index, row in all_problems.iterrows():
    print(f"{row['data_set_id']} -> {row['problem_id']}")

    data_set_id = row['data_set_id']
    problem_id = row['problem_id']

    avg = []
    for i in range(0, runs):
        behave_query = f"""-- join all three tables
        SELECT 
            o.*,
            i.source_code as program_code,
            t.source_code as test_code,
            i.language
        FROM iceberg_scan('{observations_table.metadata_location}') o
        INNER JOIN iceberg_scan('{code_implementations_table.metadata_location}') i
            ON o.implementation_id = i.implementation_id
            AND o.data_set_id = i.data_set_id
            AND o.problem_id = i.problem_id
        INNER JOIN iceberg_scan('{tests_table.metadata_location}') t
            ON o.test_id = t.test_id
            AND o.data_set_id = t.data_set_id
            AND o.problem_id = t.problem_id
        WHERE o.data_set_id = '{data_set_id}'
            AND o.problem_id = '{problem_id}'
            AND i.data_set_id = '{data_set_id}'
            AND i.problem_id = '{problem_id}'
            AND t.data_set_id = '{data_set_id}'
            AND t.problem_id = '{problem_id}'
        """

        start_time = time.perf_counter()

        result = analyzer.conn.execute(behave_query).fetchdf()

        end_time = time.perf_counter()

        execution_time_ms = (end_time - start_time) * 1000

        print(f"time msecs = {execution_time_ms}")

        avg.append(execution_time_ms)

        benchmarks.append(QueryBenchmark(
            problem_id=problem_id,
            query_type='three-way-join',
            impls=-1,
            tests=-1,
            execution_time_ms=execution_time_ms,
            timestamp=datetime.now()
        ))

    print(f"mean = {statistics.mean(avg)}")

df = pd.DataFrame([asdict(b) for b in benchmarks])
df.to_csv("benchmark_three_way_join.csv")

print(df.describe())