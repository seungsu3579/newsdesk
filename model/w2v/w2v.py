import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from konlpy.tag import Mecab
from gensim.models import Word2Vec
from tool.preprocess import Preprocess
from tqdm import tqdm


class WordToVector:
    """
    controller of word2vector model

    WordToVector model default attribute :
        v_dimension = 300
        v_window = 8
        min_count = 5
        workers = 4
        sg = 0

    """

    def __init__(self):
        self.model = Word2Vec.load("word2vec.model")
        self.v_dimension = 300
        self.v_window = 8
        self.min_count = 5
        self.workers = 4
        self.sg = 0

    def update_model(self, file):
        """
        Input : list of word list  preprocessed sentences
        """

        pre = Preprocess()
        mecab = Mecab()

        # load new data
        df = pre.data_loader(file)
        df = pre.sentence_process(df)

        # get stopwords
        stopwords = pre.load_stopwords()

        news_tokens = []
        # for i in tqdm(range(len(df))):
        for i in tqdm(range(10)):
            for sentence in df["sentence"][i]:
                news_tokens.append(
                    list(map(lambda x: x.split("/")[0], pre.mecab_tokenizer(sentence)))
                )

        # tranfer learning

        self.model.build_vocab(news_tokens, update=True)
        self.model.train(news_tokens, total_examples=len(news_tokens), epochs=10)


if __name__ == "__main__":
    w = WordToVector()
    print(path.abspath(__file__))
    w.update_model("../tmp/test.csv")