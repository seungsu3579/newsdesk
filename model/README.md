## NLP model

키워드 기반 객관적인 뉴스 추출 모델

### Flow

1. 지난 많은 뉴스 데이터로 모델 학습

2. 개별 문서를 모델을 통해 토큰화 / 벡터화

3. text rank 활용하여 핵심 문장/키워드를 추출 > db로 핵심문장(요약), 키워드 저장

4. 핵심 문장을 벡터화 (문장을 벡터화) : tf-idf doc2Vec word2Vec

5. 벡터화된 문장을 k means 또는 cosine similarity를 통해 군집화

6. 군집화된 뉴스데이터의 키워드를 db 에서 불러오고, rank로 군집내에서 가장 많은 키워드를 확인

7. 빈도수가 많은 거를 가장 객관적인 것으로

키워드를 뽑기 : tokenizer(spark ko nlp)

문서 벡터화 :
