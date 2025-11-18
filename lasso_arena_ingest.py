import time

from olake.ingest.arena import ArenaDataIngestionOptimized, ArenaCodeUnitIngestion
from olake.lakehouse import ObservationLakehouse, ObservationAnalyzer

lakehouse = ObservationLakehouse(
    warehouse_path="./warehouse",
    catalog_db_path="iceberg_catalog.db"
)

# Create tables if needed
lakehouse.create_observations_table()
lakehouse.create_code_table()
lakehouse.create_test_table()

# Initialize arena ingestion handler
arena_ingestion = ArenaDataIngestionOptimized(lakehouse)
arena_code_ingestion = ArenaCodeUnitIngestion(lakehouse)

# track time
start_time = time.perf_counter()

# import code implementations
arena_code_ingestion.ingest_code_implementations(
    "code_candidates/HUMANEVAL_solr_export.json",
    "HumanEval"
)

print(f"code HUMANEVAL {(time.perf_counter() - start_time) * 1000}")
start_time = time.perf_counter()

# import observations
arena_ingestion.ingest_observations(
    "39c2236c-613c-4d0a-881c-bd294f6eef7a_all.parquet",
    "HumanEval"
)

print(f"obs HUMANEVAL {(time.perf_counter() - start_time) * 1000}")
start_time = time.perf_counter()

# import test specifications
arena_ingestion.ingest_tests(
    "39c2236c-613c-4d0a-881c-bd294f6eef7a_all.parquet",
    "HumanEval"
)

print(f"tests HUMANEVAL {(time.perf_counter() - start_time) * 1000}")
start_time = time.perf_counter()

# import code implementations
arena_code_ingestion.ingest_code_implementations(
    "code_candidates/MBPP_solr_export.json",
    "MBPP"
)

print(f"code MBPP {(time.perf_counter() - start_time) * 1000}")
start_time = time.perf_counter()

# import observations
arena_ingestion.ingest_observations(
    "14c387a3-dde2-4b9a-b1ee-deae5f8ac0de_all.parquet",
    "MBPP"
)

print(f"obs MBPP {(time.perf_counter() - start_time) * 1000}")
start_time = time.perf_counter()

# import tests
arena_ingestion.ingest_tests(
    "14c387a3-dde2-4b9a-b1ee-deae5f8ac0de_all.parquet",
    "MBPP"
)

end_time = time.perf_counter()

execution_time_ms = (end_time - start_time) * 1000

print(f"tests MBPP {execution_time_ms}")

# Analyze the ingested data
analyzer = ObservationAnalyzer(lakehouse)
stats = analyzer.query_observations("""
    SELECT 
        data_set_id,
        problem_id,
        COUNT(DISTINCT implementation_id) as num_implementations,
        COUNT(DISTINCT test_id) as num_tests,
        COUNT(*) as total_observations
    FROM observations
    WHERE run_id IS NOT NULL
    GROUP BY data_set_id, problem_id
    ORDER BY total_observations DESC
    LIMIT 10
""")

print(stats)