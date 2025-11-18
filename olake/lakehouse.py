import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import SortOrder
from pyiceberg.table.sorting import SortField, SortDirection
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import duckdb
from datetime import datetime
from pathlib import Path


class ObservationLakehouse:
    """
    Lakehouse for realizing continual SRC.
    Supports schema evolution and multi-source data integration.
    """

    def __init__(self, warehouse_path: str, catalog_db_path: str = "iceberg_catalog.db"):
        """Initialize the lakehouse with catalog and warehouse locations."""
        self.warehouse_path = Path(warehouse_path)
        self.warehouse_path.mkdir(parents=True, exist_ok=True)

        # Initialize Iceberg catalog (SQLite-based for local development)
        self.catalog = load_catalog(
            "local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{catalog_db_path}",
                "warehouse": f"file://{self.warehouse_path.absolute()}",
            },
        )

        # Define the core observation schema
        self.schema = self._create_core_schema()
        # Code schema
        self.code_schema = self._create_code_schema()
        # Test schema
        self.test_schema = self._create_test_schema()

    def _create_core_schema(self) -> Schema:
        """
        Create the core observation schema with dimensional extensibility.
        This schema captures the core dimensions from the SRC concept.
        """
        return Schema(
            # Core identifier dimensions
            NestedField(1, "data_set_id", StringType(), required=True),
            NestedField(2, "problem_id", StringType(), required=True),
            NestedField(3, "implementation_id", StringType(), required=True),
            NestedField(4, "test_id", StringType(), required=True),

            # hash
            NestedField(5, "implementation_hash", StringType(), required=True),
            NestedField(6, "test_hash", StringType(), required=True),

            NestedField(7, "run_id", StringType(), required=True),
            NestedField(8, "environment_id", StringType(), required=True),

            # Sequence sheet data (execution flow) [1]
            NestedField(9, "step_id", IntegerType(), required=True),
            # signature
            NestedField(10, "operation", StringType()),
            NestedField(11, "inputs", StringType()),  # JSON-serialized
            NestedField(12, "output", StringType()),  # JSON-serialized

            # Non-functional metrics [1]
            NestedField(13, "execution_time_ms", DoubleType()),
            NestedField(14, "memory_used_mb", DoubleType()),
            NestedField(15, "branch_coverage_percent", DoubleType()),

            # Metadata for tracking
            NestedField(16, "created_at", TimestampType()),

            # Optional dimensions (for future extensibility)
            # CI/Git metadata (initially NULL for non-CI sources)
            NestedField(17, "git_commit_hash", StringType(), required=False),
            NestedField(18, "ci_pipeline_id", StringType(), required=False),

            # Study metadata (initially NULL for non-study sources)
            NestedField(19, "researcher_name", StringType(), required=False),

            # is oracle
            NestedField(20, "specified_oracle", BooleanType(), required=False),
        )

    def _create_code_schema(self) -> Schema:
        """
        Create the code schema
        """
        return Schema(
            NestedField(1, "data_set_id", StringType(), required=True),
            NestedField(2, "problem_id", StringType(), required=True),
            NestedField(3, "implementation_id", StringType(), required=True),
            NestedField(4, "source_code", StringType(), required=True),
            NestedField(5, "code_hash", StringType()),  # For deduplication
            NestedField(6, "created_at", TimestampType()),
            # Optional: code metadata
            NestedField(7, "lines_of_code", IntegerType()),
            NestedField(8, "cyclomatic_complexity", IntegerType()),

            # metadata
            NestedField(9, "language", StringType()),
        )

    def _create_test_schema(self) -> Schema:
        """
        Create the code schema
        """
        return Schema(
            NestedField(1, "data_set_id", StringType(), required=True),
            NestedField(2, "problem_id", StringType(), required=True),
            NestedField(3, "test_id", StringType(), required=True),
            NestedField(4, "source_code", StringType(), required=True),
            NestedField(5, "focal_interface", StringType(), required=True),
            NestedField(6, "code_hash", StringType()),  # For deduplication
            NestedField(7, "created_at", TimestampType()),
            # Optional: code metadata
            #NestedField(7, "lines_of_code", IntegerType()),
            #NestedField(8, "cyclomatic_complexity", IntegerType()),

            # metadata
            NestedField(9, "language", StringType()),
        )

    def create_observations_table(self, namespace: str = "db"):
        """
        Create the main observations table with partitioning strategy.
        Partitions by data_set_id and problem_id for efficient queries [1].
        """
        try:
            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(namespace)
            except:
                pass  # Namespace might already exist

            # Define partitioning strategy
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=1,  # data_set_id
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="data_set_id"
                ),
                PartitionField(
                    source_id=2,  # problem_id
                    field_id=1001,
                    transform=IdentityTransform(),
                    name="problem_id"
                )
            )

            # FIXME z-ordering (only do this from time to time to avoid rewrites!)
            # sort by implementation then by test, then by step
            # sort_order = SortOrder(
            #     fields=[
            #         SortField(source_id=3, sort_order=SortDirection.ASC, transform=IdentityTransform()), # implementation
            #         SortField(source_id=4, transform=IdentityTransform()), # test
            #         #SortField(source_id=6, transform=IdentityTransform()), # step (optional)
            #     ]
            # )

            # Create the table
            table = self.catalog.create_table(
                identifier=f"{namespace}.observations",
                schema=self.schema,
                partition_spec=partition_spec,
                #sort_order=sort_order
            )

            print(f"✓ Created observations table: {namespace}.observations")
            return table

        except Exception as e:
            print(f"Table might already exist or error occurred: {e}")
            return self.catalog.load_table(f"{namespace}.observations")

    def create_code_table(self, namespace: str = "db"):
        """
        Create the main code table with partitioning strategy.
        Partitions by data_set_id and problem_id for efficient queries.
        """
        try:
            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(namespace)
            except:
                pass  # Namespace might already exist

            # Define partitioning strategy
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=1,  # data_set_id
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="data_set_id"
                ),
                PartitionField(
                    source_id=2,  # problem_id
                    field_id=1001,
                    transform=IdentityTransform(),
                    name="problem_id"
                )
            )

            # Create the table
            table = self.catalog.create_table(
                identifier=f"{namespace}.code_implementations",
                schema=self.code_schema,
                partition_spec=partition_spec
            )

            print(f"✓ Created code table: {namespace}.code_implementations")
            return table

        except Exception as e:
            print(f"Table might already exist or error occurred: {e}")
            return self.catalog.load_table(f"{namespace}.code_implementations")


    def create_test_table(self, namespace: str = "db"):
        """
        Create the main test table with partitioning strategy.
        Partitions by data_set_id and problem_id for efficient queries.
        """
        try:
            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(namespace)
            except:
                pass  # Namespace might already exist

            # Define partitioning strategy
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=1,  # data_set_id
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="data_set_id"
                ),
                PartitionField(
                    source_id=2,  # problem_id
                    field_id=1001,
                    transform=IdentityTransform(),
                    name="problem_id"
                )
            )

            # Create the table
            table = self.catalog.create_table(
                identifier=f"{namespace}.tests",
                schema=self.test_schema,
                partition_spec=partition_spec
            )

            print(f"✓ Created tests table: {namespace}.tests")
            return table

        except Exception as e:
            print(f"Table might already exist or error occurred: {e}")
            return self.catalog.load_table(f"{namespace}.tests")



    def load_observations_table(self, namespace: str = "db"):
        """Load the observations table."""
        return self.catalog.load_table(f"{namespace}.observations")

    def load_code_table(self, namespace: str = "db"):
        """Load the code table."""
        return self.catalog.load_table(f"{namespace}.code_implementations")

    def load_test_table(self, namespace: str = "db"):
        """Load the code table."""
        return self.catalog.load_table(f"{namespace}.tests")

    def evolve_observations_schema(self, new_column_name: str, new_column_type, namespace: str = "db"):
        """
        Add a new dimension to the schema (schema evolution).
        Demonstrates dimensional extensibility.
        """
        table = self.load_observations_table(namespace)

        # Get next field ID
        max_field_id = max(field.field_id for field in self.schema.fields)

        with table.update_schema() as update:
            update.add_column(
                new_column_name,
                new_column_type,
                required=False
            )

        print(f"✓ Added new dimension: {new_column_name}")

    def evolve_code_schema(self, new_column_name: str, new_column_type, namespace: str = "db"):
        """
        Add a new dimension to the schema (schema evolution).
        Demonstrates dimensional extensibility [1].
        """
        table = self.load_code_table(namespace)

        # Get next field ID
        max_field_id = max(field.field_id for field in self.schema.fields)

        with table.update_schema() as update:
            update.add_column(
                new_column_name,
                new_column_type,
                required=False
            )

        print(f"✓ Added new dimension: {new_column_name}")

    def evolve_test_schema(self, new_column_name: str, new_column_type, namespace: str = "db"):
        """
        Add a new dimension to the schema (schema evolution).
        Demonstrates dimensional extensibility.
        """
        table = self.load_test_table(namespace)

        # Get next field ID
        max_field_id = max(field.field_id for field in self.schema.fields)

        with table.update_schema() as update:
            update.add_column(
                new_column_name,
                new_column_type,
                required=False
            )

        print(f"✓ Added new dimension: {new_column_name}")


class ObservationIngestion:
    """
    Handles ingestion from various sources (Arena, CI, etc.) into the lakehouse.
    """

    def __init__(self, lakehouse: ObservationLakehouse):
        self.lakehouse = lakehouse


class ObservationAnalyzer:
    """
    Query and analyze the observation lakehouse using DuckDB.
    """

    def __init__(self, lakehouse: ObservationLakehouse):
        self.lakehouse = lakehouse
        self.conn = duckdb.connect(":memory:")

        # Optimize DuckDB settings
        #self.conn.execute("SET threads TO 16")  # Use all cores
        #self.conn.execute("SET memory_limit = '16GB'")

        # Install and load Iceberg extension
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")

    def query_observations(self, sql: str):
        """Execute a SQL query against the lakehouse."""
        # Load table as DuckDB relation
        table = self.lakehouse.load_observations_table()

        # Scan to PyArrow and register in DuckDB
        arrow_table = table.scan().to_arrow()
        self.conn.register("observations", arrow_table)

        result = self.conn.execute(sql).fetchdf()
        return result

    def query_observations_duck(self, sql: str):
        """Execute a SQL query against the lakehouse."""
        # Load table as DuckDB relation
        table = self.lakehouse.load_observations_table()

        # Scan to PyArrow and register in DuckDB
        arrow_table = table.scan().to_arrow()
        self.conn.register("observations", arrow_table)

        result = self.conn.execute(sql).fetchall()
        return result

    def query_code(self, sql: str):
        """Execute a SQL query against the lakehouse."""
        # Load table as DuckDB relation
        table = self.lakehouse.load_code_table()

        # Scan to PyArrow and register in DuckDB
        arrow_table = table.scan().to_arrow()
        self.conn.register("code_implementations", arrow_table)

        result = self.conn.execute(sql).fetchdf()
        return result

    def query_tests(self, sql: str):
        """Execute a SQL query against the lakehouse."""
        # Load table as DuckDB relation
        table = self.lakehouse.load_test_table()

        # Scan to PyArrow and register in DuckDB
        arrow_table = table.scan().to_arrow()
        self.conn.register("tests", arrow_table)

        result = self.conn.execute(sql).fetchdf()
        return result