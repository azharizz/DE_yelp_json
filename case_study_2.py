import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psutil
import os

def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 * 1024)  # Return memory in MB

def etl():
    mem_before = get_memory_usage()
    print(f"Memory usage before: {mem_before:.2f} MB")


    chunksize=20000 

    pqwriter = None
    for i, df in enumerate(pd.read_json('yelp_academic_dataset_review.json', chunksize=chunksize, lines=True)):
        print(f"chunk {i}")
        table = pa.Table.from_pandas(df)

        # for the first chunk of records
        if i == 0:
            # create a parquet write object giving it an output file
            pqwriter = pq.ParquetWriter('sample.parquet', table.schema)            
        pqwriter.write_table(table)

    pqwriter.close()


    # Get memory usage after
    mem_after = get_memory_usage()
    print(f"Memory usage after: {mem_after:.2f} MB")

    # Memory difference
    mem_diff = mem_after - mem_before
    print(f"Memory difference: {mem_diff:.2f} MB")

etl()