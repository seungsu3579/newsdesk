import sys, os
from tqdm import tqdm
import pandas as pd
from .summarizer import KeysentenceSummarizer
from .summarizer import KeywordSummarizer
from tool.preprocess import Preprocess
# from ...crawler.crawler.articlecrawler import upload_s3_csv


# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RunTextRank(object):
    def __init__(self):
        self.preprocess = Preprocess()
        self.sentence_summarizer = KeysentenceSummarizer(
            tokenize=self.preprocess.mecab_tokenizer, min_sim=0.5, verbose=False
        )
        self.word_summarizer = KeywordSummarizer(
            tokenize=self.preprocess.mecab_tokenizer, min_count=5, min_cooccurrence=2
        )

    def run_data_loader(self, file):
        data = self.preprocess.data_loader(file)
        data_result = self.preprocess.sentence_process(data)
        return data_result

    def get_keysentence(self, sent: list):
        sentence = self.sentence_summarizer.summarize(sent, topk=3)
        return sentence

    def get_keyword(self, sent: list):
        keyword = self.word_summarizer.summarize(sent, topk=5)
        return keyword

    def data_saver(self, file):
        sentence_list = []
        word_list = []
        data = self.run_data_loader(file=file)
        for i_index in tqdm(range(len(data))):
            sentence_list.append(self.get_keysentence(data["sentence"][i_index]))
            word_list.append(self.get_keyword(data["sentence"][i_index]))
        data["keysentence"] = sentence_list
        data["keyword"] = word_list

        return data
        

if __name__ == "__main__":
    run = RunTextRank()
    df = run.data_saver("../tmp/test.csv")