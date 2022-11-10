# A simple tool to coalesce a list of parquet files into a single parquet file or a list of parquet files of a given size
# Usage: python main.py -s 128 -i '/path/to/input/*' -o /path/to/output

import duckdb
import argparse
import logging
import duckdb
import os
import tempfile
import glob
import shutil

# create a database from a path with duckdb
def create_database(path):
    db = duckdb.connect(path)
    return db

# create a table ingest from a list of parquet files from a path
def create_table(db, table_name: str, paths):
    paths_list = ",".join([f"'{path}'" for path in paths])
    db.execute(
        f"CREATE TABLE {table_name} AS SELECT * FROM parquet_scan([{paths_list}]);")

# export a table as a parquet file to a path
def export_table(db, table_name, path):
    db.execute(f"COPY (SELECT * FROM {table_name}) TO '{path}' (FORMAT PARQUET,CODEC 'SNAPPY');")


# diplays statistics about the parquet file
def display_parquet_stats(db,path):
    print(f"Displaying statistics about {path}")
    df = db.execute(f"SELECT count(file_name) as number_files,sum(total_compressed_size)/1024 as sum_compressed,sum(total_uncompressed_size)/1024 as sum_uncompressed FROM parquet_metadata('{path}');").fetchdf()
    # display the dataframe df
    print(df)

# get a list of files from a path using a glob utility
def get_files(path: str) -> [str]: 
    files = glob.glob(path)
    # get an array of string from the list of files
    filesArray = [str(file) for file in files]
    return filesArray

# calculate the sum of size of a list of files
def get_size(files):
    size = 0
    for file in files:
        size += os.path.getsize(file)
    return size

# get number of lines in table 
def get_number_lines(db, table_name: str) -> int:
    return db.execute(f"SELECT count(*) FROM {table_name};").fetchdf().values[0][0]

# export a parquet file from a database and table from line i to line j = i + size
def export_table_from_to(db, table_name: str, path: str, offset: int, number_lines: int):
    db.execute(f"COPY (SELECT * FROM {table_name} LIMIT {number_lines} OFFSET {offset}) TO '{path}' (FORMAT PARQUET,CODEC 'SNAPPY');")

# calculate the number of line to export from the total size and the total of lines to export 128M by file  
def calculate_number_lines(total_size_in_bytes : int, total_lines: int, size_by_file_in_mb: int) -> int:
    size_by_line_in_byte = total_size_in_bytes / total_lines
    size_by_file_in_byte = size_by_file_in_mb * 1024 * 1024
    number_lines = size_by_file_in_byte / size_by_line_in_byte
    return int(number_lines)

# export a parquet file from a database and table from line i to line j = i + size
def export_table(db, table_name: str,dest_path: str, total_lines: int, number_of_lines_by_file: int):
    ensure_dir(dest_path)
    for i in range(0, total_lines, number_of_lines_by_file):
        path = f"{dest_path}/{i}_{i+number_of_lines_by_file}.parquet"
        export_table_from_to(db, table_name, path, i, number_of_lines_by_file)

# get the database size in bytes
def get_database_size(db) -> int:
    strSize = db.execute("PRAGMA database_size;").fetchdf().values[0][0]
    if strSize.endswith("GB"):
        return int(float(strSize[:-2])* 1024 * 1024 * 1024)
    if(strSize.endswith("MB")):
        size = int(float(strSize[:-2]) * 1024 * 1024)
    elif(strSize.endswith("KB")):
        size = int(float(strSize[:-2]) * 1024)
    else:
        size = int(float(strSize[:-1]))
    return size

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# clean the directory
def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)

# check if file exists in directory
def file_exists_in_directory(path):
    # list all files in the directory
    files = os.listdir(path)
    if len(files) > 0:
        return True

# Python CLI main function
def main():
    parser = argparse.ArgumentParser(
        prog='duckdb_coalesce_parquet',
        description='A Python CLI to coalesce parquet files into multiple parquet files with a defined size',
        epilog='Enjoy the program! :)')

    # positional argument source file path
    parser.add_argument('sourcePath', type=str, help="The source path to the parquet files usin a glob pattern example: '/path/to/input/*'")
    # positional argument destination file path
    parser.add_argument('destinationPath', type=str, help='The destination path to the parquet files')
    # optional argument size by file in MB (default 128)
    parser.add_argument('--size', type=int, default=128, help='The size of the parquet files in MB (default 128). Example --size 128')
    # optional argument to clean the destination path (default False)
    parser.add_argument('--clean', type=bool, default=False, help='Clean the destination path before exporting the parquet files. Default False. Example --clean True')

    args = parser.parse_args()
    print(args.sourcePath, args.destinationPath, args.size)

    # get list of files from the source path
    files = get_files(args.sourcePath)

    # if no files found, exit with error
    if len(files) == 0:
        print(f"No files found in the source path ${args.sourcePath}")
        return 1

    # if the destination path exists and clean is true, clean the directory
    if args.clean and file_exists_in_directory(args.destinationPath):
        clean_dir(args.destinationPath)
    elif file_exists_in_directory(args.destinationPath):
        print(f"Destination path ${args.destinationPath} already exists and --clean option is false. Exiting.")
        return 1

    with tempfile.TemporaryDirectory() as tmpdirname:
        print(f"Created temporary directory {tmpdirname}")
        # create a database from a path
        database_ingest_path = os.path.join(tmpdirname, 'ingest_database.db')
        print(f"creating database from database_ingest_path: {database_ingest_path}")

        # calculate the size of the files
        size = get_size(files)
        sizeInMB = size / 1024 / 1024
        print(f"size of the files: {sizeInMB} MB")
        print(f"size of the files: {size} bytes")
        # create a database from a path
        db = create_database(database_ingest_path) 
        # create a table ingest from a list of parquet files from a path
        print(f"Creating table from {args.sourcePath}")
        create_table(db, "ingest", files)
         # display the number of lines in the ingest table 
        number_lines = get_number_lines(db, "ingest")
        print(f"number of lines in the ingest table: {number_lines}")
        # calculate the number of lines to export from the total size and the total of lines to export 128M by file
        database_size = get_database_size(db)
        print(f"database size: {database_size}")
        number_lines_to_export = calculate_number_lines(database_size, number_lines, int(args.size))
        # display the number of lines to export
        print(f"number of lines to export: {number_lines_to_export}")
        # export a table as a parquet file to a path
        print(f"Exporting table to {args.destinationPath}")
        export_table(db, "ingest", args.destinationPath, number_lines, number_lines_to_export)
        db.close()
        print("Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e)
