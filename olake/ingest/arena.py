import hashlib
import json
from datetime import datetime

import duckdb
import pyarrow as pa

from olake.lakehouse import ObservationLakehouse, ObservationIngestion


def normalize_source(data: str):
    """
    Normalize source (prepare for hash)

    :param data:
    :return:
    """

    text = data
    # normalize newlines to '\n'
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # strip trailing whitespace on each line and remove trailing blank lines
    lines = [ln.rstrip() for ln in text.split('\n')]
    # remove trailing blank lines
    while lines and lines[-1] == '':
        lines.pop()
    # join with single '\n'
    return '\n'.join(lines).encode('utf-8')


def git_blob_hash(content: str):
    """
    Git-like hashing of contents

    :param content:
    :return:
    """

    text = normalize_source(content)
    header = f'blob {len(text)}\0'.encode('utf-8')
    h = hashlib.sha1()
    h.update(header)
    h.update(text)

    return h.hexdigest()


class ArenaDataIngestionOptimized(ObservationIngestion):
    """
    LASSO Arena ingestion using DuckDB for Parquet reading.
    """
    def __init__(self, lakehouse: ObservationLakehouse):
        super().__init__(lakehouse)
        # Create a persistent DuckDB connection for the ingestion session
        self.duckdb_conn = duckdb.connect(":memory:")


    def ingest_observations(self, parquet_pattern: str,
                                    data_set_id: str = "Arena"):
        """
        Ingest multiple LASSO Arena parquet files using glob patterns.
        DuckDB can read multiple files in a single query for even better performance.

        See also https://softwareobservatorium.github.io/web/hub for LASSO parquet file examples.
        See also https://softwareobservatorium.github.io/web/docs/intro for LASSO documentation

        Args:
            parquet_pattern: Glob pattern (e.g., './arena_exports/*.parquet')
            data_set_id: Data set identifier
        """

        query = f"""
        WITH grouped_observations AS (
            SELECT 
                EXECUTIONID,
                ABSTRACTIONID,
                SYSTEMID,
                VARIANTID,
                ADAPTERID,
                SHEETID,
                ARENAID,
                Y as step_id,
                STRING_AGG(
                    CASE WHEN TYPE = 'input_value' THEN VALUE END, 
                    ',' ORDER BY X
                ) as inputs_array,
                STRING_AGG(
                    CASE WHEN TYPE = 'value' THEN VALUE END, 
                    ',' ORDER BY X
                ) as outputs_array,
                MAX(CASE WHEN TYPE = 'op' THEN VALUE END) as operation,
                MAX(EXECUTIONTIME) as execution_time,
                MAX(CASE WHEN SYSTEMID = 'oracle' THEN TRUE ELSE FALSE END) as specified_oracle,
            FROM read_parquet('{parquet_pattern}')  -- Glob pattern!
            -- FILTER
            WHERE
                Y > -1 -- ignore -1
                AND SYSTEMID != 'oracle'
            GROUP BY EXECUTIONID, ABSTRACTIONID, SYSTEMID, VARIANTID, 
                     ADAPTERID, SHEETID, ARENAID, Y
        )
        SELECT 
            '{data_set_id}' as data_set_id,
            ABSTRACTIONID as problem_id,
            CONCAT(SYSTEMID, '_', COALESCE(NULLIF(VARIANTID, ''), 'default'), 
                   '_', ADAPTERID) as implementation_id,
            SHEETID as test_id,
            '' as implementation_hash,
            '' as test_hash,
            EXECUTIONID as run_id, -- FIXME
            ARENAID as environment_id,
            step_id,
            operation,
            inputs_array as inputs,
            outputs_array as output,
            CAST(execution_time AS DOUBLE) as execution_time_ms,
            CAST(NULL AS DOUBLE) as memory_used_mb,
            CAST(NULL AS DOUBLE) as branch_coverage_percent,
            CURRENT_TIMESTAMP as created_at,
            CAST(NULL AS VARCHAR) as git_commit_hash,
            CAST(NULL AS VARCHAR) as ci_pipeline_id,
            CAST(NULL AS VARCHAR) as researcher_name,
            specified_oracle
        FROM grouped_observations
        """

        result = self.duckdb_conn.execute(query)
        arrow_batch = result.arrow()

        batches = list(arrow_batch)
        total_rows = sum(batch.num_rows for batch in batches)

        # cast schema
        schema = self.lakehouse.schema.as_arrow()
        aligned_batches = [b.cast(schema) for b in batches]

        # Single append for all files
        table = self.lakehouse.load_observations_table()

        # Convert to PyArrow Table with correct schema
        arrow_table = pa.Table.from_batches(
            aligned_batches,
            schema=schema
        )

        table.append(arrow_table)

        print(f"✓ Ingested {arrow_table.num_rows} observations from pattern: {parquet_pattern}")
        return arrow_table.num_rows

    def ingest_tests(self, parquet_pattern: str,
                                    data_set_id: str = "Arena"):
        """
        Ingest LASSO actuation sheets (from LASSO exported parquet files).
        DuckDB can read multiple files in a single query for even better performance.

        Args:
            parquet_pattern: Glob pattern (e.g., './arena_exports/*.parquet')
            data_set_id: Data set identifier
        """

        query = f"""
        WITH grouped_observations AS (
            SELECT 
                EXECUTIONID,
                ABSTRACTIONID,
                SHEETID,
                MAX(CASE WHEN TYPE = 'stimulussheet' THEN VALUE END) as source_code,
                MAX(CASE WHEN TYPE = 'interface' THEN VALUE END) as focal_interface
            FROM read_parquet('{parquet_pattern}')  -- Glob pattern!
            -- FILTER
            WHERE
                Y = -1 AND SYSTEMID = 'abstraction' AND (TYPE = 'stimulussheet' OR TYPE = 'interface')
            GROUP BY EXECUTIONID, ABSTRACTIONID, SHEETID
        )
        SELECT 
            '{data_set_id}' as data_set_id,
            ABSTRACTIONID as problem_id,
            SHEETID as test_id,
            source_code,
            focal_interface,
            '' as code_hash,
            CURRENT_TIMESTAMP as created_at,
            'java' as language
        FROM grouped_observations
        WHERE
            source_code IS NOT NULL
        """

        result = self.duckdb_conn.execute(query)
        arrow_batch = result.arrow()

        batches = list(arrow_batch)
        total_rows = sum(batch.num_rows for batch in batches)

        # table + schema
        table = self.lakehouse.load_test_table()
        schema = self.lakehouse.test_schema.as_arrow()
        modified_batches = []
        for batch in batches:
            # Compute code_hash from source_code
            source_code_array = batch.column('source_code')
            code_hashes = []

            for i in range(len(source_code_array)):
                source_code = source_code_array[i].as_py()
                hash_value = git_blob_hash(source_code)
                code_hashes.append(hash_value)

            code_hash_array = pa.array(code_hashes, type=pa.string())

            # Replace the empty code_hash column
            batch_with_hash = batch.set_column(
                batch.schema.get_field_index('code_hash'),
                'code_hash',
                code_hash_array
            )

            # Cast to Iceberg schema (this preserves field IDs)
            aligned_batch = batch_with_hash.cast(schema)
            modified_batches.append(aligned_batch)

        # Convert to PyArrow Table with correct schema
        arrow_table = pa.Table.from_batches(
            modified_batches,
            schema=schema
        )

        table.append(arrow_table)

        print(f"✓ Ingested {arrow_table.num_rows} tests from pattern: {parquet_pattern}")
        return arrow_table.num_rows


class ArenaCodeUnitIngestion:
    """
    Parses Arena JSON exports (from LASSO's Code Index based on Solr/Lucene) and ingests code units into the
    code_implementations table.
    """

    def __init__(self, lakehouse: ObservationLakehouse):
        self.lakehouse = lakehouse


    def parse_arena_json(self, json_path: str, data_set_id: str) -> list:
        """
        Parse Arena JSON export into code implementation records.

        Args:
            json_path: Path to Arena JSON export file

        Returns:
            List of code implementation records
            :param json_path: Path to json file
            :param data_set_id: Data set identifier
        """
        with open(json_path, 'r') as f:
            arena_data = json.load(f)

        implementations = []

        # Solr JSON response
        for doc in arena_data.get('response').get('docs', []):
            abstraction_name = doc.get('abstractionId')[0]

            impl_record = self._transform_code_unit(doc, data_set_id, abstraction_name)
            implementations.append(impl_record)

        return implementations

    def _transform_code_unit(self, doc: dict, data_set_id: str, abstraction_name: str) -> dict:
        """
        Transform a single Arena code unit into lakehouse 'code_implementations' format.

        Args:
            doc: Solr JSON document entry
            data_set_id: Data set identifier
            abstraction_name: Name of the functional abstraction

        Returns:
            Dictionary matching code_implementations schema
        """
        # Extract measures


        # Build the record
        record = {
            'data_set_id': data_set_id,
            'problem_id': abstraction_name,
            'language': doc.get('lang', '').lower(),
            'implementation_id': f"{doc.get('id')}_original_0", # FIXME flexible handling of variantId, adapterId
            'source_code': doc.get('content', ''),
            'code_hash': git_blob_hash(doc.get('content', '')),
            'created_at': datetime.now(),
            # Quality metrics
            'lines_of_code': int(doc.get('m_static_loc_td', [])[0]),
            'cyclomatic_complexity': doc.get('m_static_complexity_td', [])[0],

            # 'source_code': code_unit.get('content', ''),
            # 'code_hash': code_unit.get('hash'),
            # 'language': code_unit.get('lang', '').lower(),
            # 'arena_id': code_unit.get('id'),
            # 'functional_abstraction_id': abstraction_name,
            # 'name': code_unit.get('name'),
            # 'package_name': code_unit.get('packagename'),
            # 'bytecode_name': code_unit.get('bytecodeName'),
            # 'group_id': code_unit.get('groupId'),
            # 'artifact_id': code_unit.get('artifactId'),
            # 'version': code_unit.get('version'),
            # 'data_source': code_unit.get('dataSource'),
            #
            # # Quality metrics
            # 'lines_of_code': int(measures.get('m_static_loc_td', 0)),
            # 'cyclomatic_complexity': measures.get('m_static_complexity_td'),
            # 'comment_lines': int(measures.get('m_static_cloc_td', 0)),
            # 'number_of_methods': int(measures.get('m_static_methods_td', 0)),
            # 'number_of_dependencies': int(measures.get('m_deps_td', 0)),
            #
            # # Structural information (serialize as JSON)
            # 'method_names': json.dumps(code_unit.get('methodNames', [])),
            # 'super_classes': json.dumps(code_unit.get('superClasses', [])),
            # 'interfaces': json.dumps(code_unit.get('interfaces', [])),
            # 'dependencies': json.dumps(code_unit.get('dependencies', [])),
            #
            # # Full measures object
            # 'measures': json.dumps(measures),
            #
            # # Metadata
            # 'ingestion_timestamp': datetime.now(),
            # 'lql_signature': code_unit.get('lql'),
        }

        return record

    def ingest_code_implementations(self, json_path: str, data_set_id: str, namespace: str = "db"):
        """
        Complete ingestion pipeline from LASSO's code index (Solr) export into code implementation records.

        Args:
            json_path: Path to Arena JSON export
            data_set_id: Data set identifier
            namespace: Lakehouse namespace
        """
        # Parse JSON
        implementations = self.parse_arena_json(json_path, data_set_id)

        print(f"Parsed {len(implementations)} code implementations from {json_path}")

        # Convert to PyArrow table
        arrow_table = pa.Table.from_pylist(
            implementations,
            schema=self.lakehouse.code_schema.as_arrow()
        )

        aligned_table = arrow_table#.cast(self.lakehouse.code_schema.as_arrow())

        # Append to lakehouse
        table = self.lakehouse.load_code_table()
        table.append(aligned_table)

        print(f"✓ Ingested {len(implementations)} code implementations")

        return implementations