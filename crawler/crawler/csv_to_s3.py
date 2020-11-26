import pyarrow as pa
from pyarrow import csv, parquet
from s3fs.core import S3FileSystem, aiobotocore

from schema import generate_schema
import os



def convert(fname, s3fsPath, error=None):
    def wrapper(*args, **kwargs):
        csvTable = csv.read_csv(fname)
        # csv안에 데이터가 없을경우 spark에서 읽어올때 스키마등 메타데이터 생성이 제대로 안됨.
        numRows = csvTable.num_rows
        if numRows > 0:
            csvTable = csvTable.cast(generate_schema(csvTable))
            parquet.write_to_dataset(table=csvTable,
                                    root_path=f"{s3fsPath}", 
                                    partition_cols=["date", "category"], 
                                    filesystem=S3FileSystem(anon=False, session=aiobotocore.AioSession(profile='hellNews')),
                                    compression="snappy")
        return "DONE"
    return wrapper()

def start():
    direct = os.path.normpath(os.getcwd() + os.sep)
    for content in os.listdir(os.getcwd()+"/data/"):
        if content == '.DS_Store':
            pass
        else: 
            convert(direct+"/data/"+content, "crawled-parquet")

if __name__ == "__main__":
    start()




