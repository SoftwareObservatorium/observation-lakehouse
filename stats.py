from olake.lakehouse import ObservationLakehouse, ObservationAnalyzer

lakehouse = ObservationLakehouse(
    warehouse_path="./warehouse",
    catalog_db_path="iceberg_catalog.db"
)

analyzer = ObservationAnalyzer(lakehouse)

iceberg_path = lakehouse.load_observations_table().metadata_location

# get overall statistics of the dataset
stats_query = f"""
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
        COUNT(problem_id),
        SUM(num_implementations),
        SUM(num_tests),
        SUM(total_observations),
        (SUM(num_implementations) / COUNT(problem_id)) as avg_impls,
        (SUM(num_tests) / COUNT(problem_id)) as avg_tests,
        (SUM(total_observations) / SUM(num_tests)) as avg_calls 
    FROM per_problem
"""

all_problems = analyzer.conn.execute(stats_query).fetchdf()

print(all_problems.to_string())

