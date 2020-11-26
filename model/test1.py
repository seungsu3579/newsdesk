from tool.preprocess import Preprocess
from tool.s3_connect import S3_connector
from textrank import run_textrank
import pandas as pd

run = run_textrank.RunTextRank()
keys = run.key_extractor()
df = run.data_saver(keys[0])
df.to_csv("test.csv",encoding="utf-8-sig")

