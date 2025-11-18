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
        behave_query = f"""-- CTE to create a single, canonical "fingerprint" for each commit.
        WITH v_behavior_signatures AS (
            SELECT
                run_id,
                problem_id,
                test_id,
                implementation_id,
                -- Get JSON string representation
                to_json(
                    list(
                        output ORDER BY step_id
                    )
                ) AS output_sequence_signature
            FROM
                iceberg_scan('{iceberg_path}')
            WHERE
                problem_id = '{problem_id}'
            GROUP BY
                run_id, problem_id, test_id, implementation_id
        ), commit_fingerprints AS (
            SELECT
                run_id,
                problem_id,
                implementation_id,
                -- Create a sorted array of all behavior signatures for this commit.
                array_agg(output_sequence_signature ORDER BY test_id) AS behavior_fingerprint,
                count(test_id) as test_size
            FROM
                v_behavior_signatures
            GROUP BY
                run_id, problem_id, implementation_id
        )
        -- Now, group commits by their identical fingerprints.
        SELECT
            -- cluster of behaviorally equivalent implementations.
            array_agg(implementation_id) AS equivalent_commits_cluster,
            -- The fingerprint that defines this cluster.
            --behavior_fingerprint,
            -- How many commits are in this cluster.
            count(*) AS cluster_size,
            MAX(test_size) as test_size
        FROM
            commit_fingerprints
        GROUP BY
            behavior_fingerprint
        ORDER BY
            cluster_size DESC
        """

        start_time = time.perf_counter()

        result = analyzer.conn.execute(behave_query).fetchdf()

        end_time = time.perf_counter()

        execution_time_ms = (end_time - start_time) * 1000

        print(f"time msecs = {execution_time_ms}")
        #print(result.to_latex())
        impls = result['cluster_size'].sum()
        tests = result['test_size'].iloc[0]
        comparisons = tests * (impls * (impls - 1) / 2)
        print(f"impls = {impls} tests = {tests}. Comparisons = {comparisons}")

        avg.append(execution_time_ms)

        benchmarks.append(QueryBenchmark(
            problem_id=problem_id,
            query_type='clustering',
            impls=impls,
            tests=tests,
            execution_time_ms=execution_time_ms,
            timestamp=datetime.now()
        ))

    print(f"mean = {statistics.mean(avg)}")

df = pd.DataFrame([asdict(b) for b in benchmarks])
df.to_csv("benchmark_behavioral_clustering.csv")

print(df.describe())