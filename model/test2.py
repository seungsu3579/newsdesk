from tool.preprocess import Preprocess
from tool.s3_connect import S3_connector
from textrank import run_textrank
import pandas as pd

run = run_textrank.RunTextRank()
keys = run.key_extractor()
s3_connector = S3_connector()
for key in keys:
    df = s3_connector.get_newsdata(key)
    print(df.head())
    