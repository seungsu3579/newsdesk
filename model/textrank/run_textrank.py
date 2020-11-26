import sys, os
from tqdm import tqdm
import pandas as pd
from .summarizer import KeysentenceSummarizer
from .summarizer import KeywordSummarizer
from tool.preprocess import Preprocess
from tool.s3_connect import S3_connector
from database import db
# from ...crawler.crawler.articlecrawler import upload_s3_csv


# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RunTextRank(object):
    def __init__(self):
        self.s3_connect = S3_connector()
        self.preprocess = Preprocess()
        self.sentence_summarizer = KeysentenceSummarizer(
            tokenize=self.preprocess.mecab_tokenizer, min_sim=0.5, verbose=False
        )
        self.word_summarizer = KeywordSummarizer(
            tokenize=self.preprocess.mecab_tokenizer, min_count=3, min_cooccurrence=2
        )

    def key_extractor(self, prefix=''):
        s3 = self.s3_connect.get_client()
        keys = self.s3_connect.get_all_newsdata_key(prefix)
        return keys

    def run_data_loader(self, key: str):
        data = self.s3_connect.get_newsdata(key)
        data_result = self.preprocess.sentence_process(data)
        return data_result

    def get_keysentence(self, sent: list):
        sentence = self.sentence_summarizer.summarize(sent, topk=3)
        return sentence

    def get_keyword(self, sent: list):
        keyword = self.word_summarizer.summarize(sent, topk=10)
        return keyword

    def data_saver(self, key):
        sentence_list = []
        word_list = []
        data = self.run_data_loader(key)
        data.reset_index(drop=True, inplace=True)
        for i_index in tqdm(range(len(data))):
            sent = [sentence[2] for sentence in self.get_keysentence(data["sentence"][i_index])]
            sentence_list.append(sent)
            word = [word[0].split('/')[0] for word in self.get_keyword(data["sentence"][i_index]) if len(word[0].split('/')[0]) !=1 ]
            word_list.append(word)
        data["keysentence"] = sentence_list
        data["keyword"] = word_list

        return data
        

if __name__ == "__main__":
    run = RunTextRank()
    df = run.data_saver("../tmp/test.csv")