# Tool: duckdb_coalesce_parquet

## What is duckdb_coalesce_parquet

A simple tool to coalesce a list of parquet files into a single parquet file or a list of parquet files of a given size
Usage: python main.py -s 128 -i '/path/to/input/*' -o /path/to/output

python main.py  '/Users/raphaelmansuy/Downloads/input_parquet/*'  ~/Downloads/output_parquet --size 256  --time True --clean True
