from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os 
from jsonschema import validate, ValidationError, Draft7Validator
from genson import SchemaBuilder  # For generating schemas
from hashlib import sha256
from typing import Callable


def generate_json_schema(json_path: str) -> dict:
    """Generate JSON Schema from a JSON file."""
    builder = SchemaBuilder()
    
    with open(json_path) as f:
        data = json.load(f)
    
    builder.add_object(data)
    return builder.to_schema()


def save_json_schema(json_path: str, output_path: str) -> dict:
    """Generate and save JSON Schema to file."""
    schema = generate_json_schema(json_path)
    
    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)
    
    return schema


def validate_json_schema(json_path: str, schema_path: str) -> tuple[bool, str]:
    """Validate JSON file against a JSON Schema."""
    with open(json_path) as f:
        data = json.load(f)
    
    with open(schema_path) as f:
        schema = json.load(f)
    
    validator = Draft7Validator(schema)
    errors = list(validator.iter_errors(data))
    
    if errors:
        return False, f"{len(errors)} errors. First: {errors[0].message[:100]}"
    return True, "OK"


class JsonToParquetConverter:
    """Convert complex nested JSON to multiple Parquet files with configurable naming."""
    
    def __init__(
        self,
        root_table_name: str,
        foreign_key_suffix: str = '_id',
        index_column_suffix: str = '_id',
        child_separator: str = '__',
        key_config: dict | None = None
    ):
        """
        Args:
            root_table_name: Name for the root table
            foreign_key_suffix: Suffix for foreign key columns (e.g., '_id' -> 'google_trends_id')
            index_column_suffix: Suffix for table's own ID column (e.g., '_id' -> 'google_trends__values_id')
            child_separator: Separator for nested table names
            key_config: Dict mapping table names to key configuration:
                - None or missing: use auto-increment index
                - str: use existing column as key
                - list[str]: generate hash from these columns
                - Callable: custom function(row) -> key value
        """
        self.root_table_name = root_table_name
        self.foreign_key_suffix = foreign_key_suffix
        self.index_column_suffix = index_column_suffix
        self.child_separator = child_separator
        self.key_config = key_config or {}
    
    def _get_index_column_name(self, table_name: str) -> str:
        """Generate index column name for a table (e.g., 'google_trends__values' -> 'google_trends__values_id')"""
        return f"{table_name}{self.index_column_suffix}"
    
    def _get_fk_column_name(self, parent_table_name: str) -> str:
        """Generate FK column name (e.g., 'google_trends' -> 'google_trends_id')"""
        return f"{parent_table_name}{self.foreign_key_suffix}"
    
    def _generate_key(self, table_name: str, row: dict, idx: int) -> str | int:
        """Generate key for a row based on key_config."""
        config = self.key_config.get(table_name)
        
        if config is None:
            return idx
        
        if isinstance(config, str):
            if config not in row:
                raise ValueError(f"Key column '{config}' not found in {table_name}. Available: {list(row.keys())}")
            return row[config]
        
        if isinstance(config, list):
            hash_input = '|'.join(str(row.get(col, '')) for col in config)
            return sha256(hash_input.encode()).hexdigest()[:16]
        
        if callable(config):
            return config(row)
        
        raise ValueError(f"Invalid key_config for {table_name}: {config}")
    
    def convert(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        compression: str = 'snappy',
        file_suffix: str = ''
    ) -> dict[str, Path]:
        """Convert JSON file to multiple Parquet files."""
        import json
        
        input_path = Path(input_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        source_file_name = input_path.name
        
        with open(input_path) as f:
            data = json.load(f)
        
        tables = self._extract_tables(data, source_file_name=source_file_name)
        output_files = {}
        
        for table_name, rows in tables.items():
            if not rows:
                continue
            
            table = pa.Table.from_pylist(rows)
            filename = f"{table_name}_{file_suffix}.parquet" if file_suffix else f"{table_name}.parquet"
            output_path = output_dir / filename
            pq.write_table(table, output_path, compression=compression)
            output_files[table_name] = output_path
        
        return output_files
    
    def inspect(self, input_path: str | Path) -> dict[str, list[str]]:
        """Inspect JSON and return detected tables with their columns."""
        import json
        
        with open(Path(input_path)) as f:
            data = json.load(f)
        
        tables = self._extract_tables(data)
        
        return {
            name: list(rows[0].keys()) if rows else []
            for name, rows in tables.items()
        }
    
    def _extract_tables(
        self,
        data: dict | list,
        table_name: str | None = None,
        parent_fk: dict | None = None,
        source_file_name: str | None = None
    ) -> dict[str, list[dict]]:
        """Recursively extract tables from nested JSON."""
        tables: dict[str, list[dict]] = {}
        table_name = table_name or self.root_table_name
        parent_fk = parent_fk or {}
        
        index_column = self._get_index_column_name(table_name)
        
        if isinstance(data, list):
            for idx, item in enumerate(data):
                if isinstance(item, dict):
                    row, nested = self._split_scalars_and_nested(item)
                    row.update(parent_fk)
                    
                    key_value = self._generate_key(table_name, row, idx)
                    row[index_column] = key_value
                    
                    if source_file_name:
                        row['source_file_name'] = source_file_name
                    
                    tables.setdefault(table_name, []).append(row)
                    
                    fk_column = self._get_fk_column_name(table_name)
                    child_fk = {**parent_fk, fk_column: key_value}
                    
                    for key, value in nested.items():
                        child_tables = self._extract_tables(
                            value,
                            table_name=f"{table_name}{self.child_separator}{key}",
                            parent_fk=child_fk,
                            source_file_name=source_file_name
                        )
                        for name, rows in child_tables.items():
                            tables.setdefault(name, []).extend(rows)
                else:
                    row = {
                        **parent_fk,
                        'value': item
                    }
                    if source_file_name:
                        row['source_file_name'] = source_file_name
                    tables.setdefault(table_name, []).append(row)
        
        elif isinstance(data, dict):
            row, nested = self._split_scalars_and_nested(data)
            row.update(parent_fk)
            
            if row:
                key_value = self._generate_key(table_name, row, 0)
                row[index_column] = key_value
                
                if source_file_name:
                    row['source_file_name'] = source_file_name
                
                tables.setdefault(table_name, []).append(row)
                
                fk_column = self._get_fk_column_name(table_name)
                child_fk = {**parent_fk, fk_column: key_value}
            else:
                child_fk = parent_fk
            
            for key, value in nested.items():
                child_tables = self._extract_tables(
                    value,
                    table_name=f"{table_name}{self.child_separator}{key}",
                    parent_fk=child_fk,
                    source_file_name=source_file_name
                )
                for name, rows in child_tables.items():
                    tables.setdefault(name, []).extend(rows)
        
        return tables
    
    @staticmethod
    def _split_scalars_and_nested(obj: dict) -> tuple[dict[str, Any], dict[str, Any]]:
        """Split dict into scalar fields and nested structures."""
        scalars = {}
        nested = {}
        
        for key, value in obj.items():
            if isinstance(value, (list, dict)):
                nested[key] = value
            else:
                scalars[key] = value
        
        return scalars, nested