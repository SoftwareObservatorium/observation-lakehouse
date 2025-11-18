from olake.lakehouse import ObservationLakehouse, ObservationAnalyzer

lakehouse = ObservationLakehouse(
    warehouse_path="./warehouse",
    catalog_db_path="iceberg_catalog.db"
)

analyzer = ObservationAnalyzer(lakehouse)

iceberg_path = lakehouse.load_observations_table().metadata_location

# statistics per problem
stats_problems_query = f"""
WITH per_problem AS (
    SELECT 
        data_set_id,
        problem_id,
        COUNT(DISTINCT implementation_id) as num_implementations,
        COUNT(DISTINCT test_id) as num_tests,
        COUNT(*) as total_observations
    FROM iceberg_scan('{iceberg_path}')
    WHERE NOT specified_oracle
    GROUP BY data_set_id, problem_id
    ORDER BY total_observations DESC
)
    SELECT
        problem_id,
        num_implementations,
        num_tests,
        total_observations,
        (total_observations / num_tests) as avg_calls 
    FROM per_problem
"""

all_problems = analyzer.conn.execute(stats_problems_query).fetchdf()

print(all_problems.to_string())

