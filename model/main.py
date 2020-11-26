from tool.preprocess import Preprocess
from tool.s3_connect import S3_connector
from textrank import run_textrank
import pandas as pd
from wordToVector import WordToVector
from tqdm import tqdm
from database.db import *

s3_connector = S3_connector()
run = run_textrank.RunTextRank()
keys = run.key_extractor()
w = WordToVector()

# word2vec 학습
# for key in tqdm(keys):
#     df = s3_connector.get_newsdata(key)
#     w.update_model(df)

# keyword
# for key in tqdm(keys):
#     df = run.data_saver(key)
#     for i in range(len(df)):
#         insert_keyword_keysentence(df['news_id'][i], df['keysentence'][i], df['keyword'][i])

ids = get_preprocessed_news_ids()
print(ids)