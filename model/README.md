## NLP model

키워드 기반 객관적인 뉴스 추출 모델

### Flow1

#### scenario 1
1. 개별 문서를 토큰화

2. 지난 많은 뉴스 데이터로 임베딩 모델 학습

3. text rank 활용하여 핵심 문장/키워드를 추출 > db로 핵심문장(요약), 키워드 저장

4. 문서별 키워드 10개를 뽑아 키워드 벡터의 평균으로 문서를 벡터화

5. 문서 벡터를 유사도 비교(코사인) 

#### scenario 2

1. 빈도수 기반으로 문서를 벡터화 : tf-idf

2. 문서 벡터를 유사도 비교


#### scenario 3

1. doc2Vec을 사용해서 문서를 벡터화

2. 문서 벡터를 유사도 비교

### Flow2

1. 벡터화된 문장을 k means 또는 cosine similarity를 통해 군집화

2. 군집화된 뉴스데이터의 키워드를 db 에서 불러오고, rank로 군집내에서 가장 많은 키워드를 확인

3. 군집안의 벡터의 평균과 가장 가까운 뉴스가 가장 객관적인 것!



키워드를 뽑기 : tokenizer(spark ko nlp)

문서 벡터화 : https://www.analyticsvidhya.com/blog/2020/08/top-4-sentence-embedding-techniques-using-python/
