import sys, os

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from konlpy.tag import Mecab
import pandas as pd
from tqdm import tqdm
import numpy as np
import re
import warnings
import boto3

warnings.filterwarnings("ignore")


class Preprocess:
    def __init__(self):
        self.mecab = Mecab()
        ## s3 directory
        # self.directory = ''

    def data_loader(self, file):
        df = pd.read_csv(file, header=None)
        df.columns = ["article"]
        df = df.drop_duplicates("article")
        df.reset_index(inplace=True, drop=True)
        return df

    def sentence_process(self, df):
        df["sentence"] = [df["article"][i].split(". ") for i in range(len(df))]

        # 각 문장의 길이가 20 이하인건 삭제
        for i in range(len(df)):
            for j in range(len(df["sentence"][i]) - 1, -1, -1):
                if len(df["sentence"][i][j]) <= 20:
                    df["sentence"][i].remove(df["sentence"][i][j])

        # 문서 중에서 문장의 길이가 10개 이상인 것만 확인
        df = df[df["sentence"].apply(lambda x: len(x) >= 10)]

        df.reset_index(inplace=True, drop=True)

        return df

    def mecab_tokenizer(self, sent):
        words = self.mecab.pos(sent, join=True)
        words = [
            w for w in words if ("/NN" in w or "/XR" in w or "/VA" in w or "/VV" in w)
        ]
        return words


if __name__ == "__main__":
    pre = Preprocess()
    # for loop file_list
    df = pre.data_loader("./tmp/test.csv")
    df = pre.sentence_process(df)

