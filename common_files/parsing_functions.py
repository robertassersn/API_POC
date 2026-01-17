from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq

def return_parquet_schema(directory,file_name):
    schema = pq.read_schema(f"{directory}/{file_name}.parquet")
    return schema

class JsonToParquetConverter:
    """Convert complex nested JSON to multiple Parquet files with configurable naming."""
    
    def __init__(
        self,
        root_table_name: str,
        foreign_key_suffix: str = '_fk',
        index_column: str = '_id',
        child_separator: str = '__'
    ):
        self.root_table_name = root_table_name
        self.foreign_key_suffix = foreign_key_suffix
        self.index_column = index_column
        self.child_separator = child_separator
    
    def convert(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        compression: str = 'snappy'
    ) -> dict[str, Path]:
        """Convert JSON file to multiple Parquet files."""
        import json
        
        input_path = Path(input_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(input_path) as f:
            data = json.load(f)
        
        tables = self._extract_tables(data)
        output_files = {}
        
        for table_name, rows in tables.items():
            if not rows:
                continue
            
            table = pa.Table.from_pylist(rows)
            output_path = output_dir / f"{table_name}.parquet"
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
        parent_fk: dict | None = None
    ) -> dict[str, list[dict]]:
        """Recursively extract tables from nested JSON."""
        tables: dict[str, list[dict]] = {}
        table_name = table_name or self.root_table_name
        parent_fk = parent_fk or {}
        
        if isinstance(data, list):
            for idx, item in enumerate(data):
                if isinstance(item, dict):
                    row, nested = self._split_scalars_and_nested(item)
                    row.update(parent_fk)
                    row[self.index_column] = idx
                    
                    tables.setdefault(table_name, []).append(row)
                    
                    fk_name = f"{table_name}{self.foreign_key_suffix}"
                    child_fk = {**parent_fk, fk_name: idx}
                    
                    for key, value in nested.items():
                        child_tables = self._extract_tables(
                            value,
                            table_name=f"{table_name}{self.child_separator}{key}",
                            parent_fk=child_fk
                        )
                        for name, rows in child_tables.items():
                            tables.setdefault(name, []).extend(rows)
                else:
                    tables.setdefault(table_name, []).append({
                        **parent_fk,
                        'value': item
                    })
        
        elif isinstance(data, dict):
            row, nested = self._split_scalars_and_nested(data)
            row.update(parent_fk)
            
            if row:
                tables.setdefault(table_name, []).append(row)
            
            for key, value in nested.items():
                child_tables = self._extract_tables(
                    value,
                    table_name=f"{table_name}{self.child_separator}{key}",
                    parent_fk=parent_fk
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