import pandas as pd
import boto3
from config import CONFIG


class S3_connector:

    def __init__(self):
        self.S3_PROFILE_NAME = CONFIG['s3_configure']['profile_name']
        self.S3_CONTENT_BUCKET = CONFIG['s3_configure']['content_bucket']
        self.S3_ACCESS_KEY_ID = CONFIG['s3_configure']['access_key_id']
        self.S3_SECRET_ACCESS_KEY = CONFIG['s3_configure']['secret_access_key']
        self.category = ['경제', '정치', '사회', '생활문화', '세계', 'IT과학', '오피니언']

    def get_client(self):
        # session = boto3.Session(p rofile_name=self.S3_PROFILE_NAME)
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.S3_ACCESS_KEY_ID,
            aws_secret_access_key=self.S3_SECRET_ACCESS_KEY,
        )

        return s3

    def get_session(self):

        session = boto3.Session(
            aws_access_key_id=self.S3_ACCESS_KEY_ID,
            aws_secret_access_key=self.S3_SECRET_ACCESS_KEY,
        )

        return session

    def get_bucket(self):

        session = self.get_session()
        bucket = session.resource("s3").Bucket(name=self.S3_CONTENT_BUCKET)
        
        return bucket

    def get_all_newsdata_key(self, prefix=''):

        """
        input  | prefix : category name   ex> '경제', '사회', ...
        return | list of s3 keys about given prefix
        """

        bucket = self.get_bucket()

        objects = list()

        for obj in bucket.object_versions.filter(Prefix=prefix).all():
            key = obj._object_key
            objects.append(key)

        return objects
    
    def get_newsdata(self, key):

        """
        input  | key : s3 file key name
        return | dataframe of key file
        """

        client = self.get_client()
        obj = client.get_object(Bucket=self.S3_CONTENT_BUCKET, Key=key)

        df = pd.read_csv(obj['Body'])
        df = df.dropna()
        df = df.drop_duplicates("content")
        df.reset_index(inplace=True, drop=True)

        return df
