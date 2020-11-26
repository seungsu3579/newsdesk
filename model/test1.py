from tool.preprocess import Preprocess
from tool.s3_connect import S3_connector

pre = Preprocess()
s3_connector = S3_connector()
test = s3_connector.data_loader