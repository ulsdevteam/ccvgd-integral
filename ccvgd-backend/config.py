import hashlib
import os


# mysql_host = "localhost"
# mysql_password = "123456"
# mysql_username = "root"
# mysql_port = 3306
# mysql_database = "ccvg_2_23"

# mysql_host = "localhost"
mysql_host = "db"
# mysql_password = "tyf8569118"
mysql_password = "123456"
mysql_username = "root"
mysql_port = 3306
mysql_database = "ccvg_5_18"



class Config:
  DEBUG = False
  JSON_AS_ASCII = False
  SQLALCHEMY_DATABASE_URL = "mysql+pymsql://root:123456@0.0.0.0:3306/sys"
  SQLALCHEMY_TRACK_MODIFICATIONS = True
  MAX_CONTENT_LENGTH = 3*1024*1924
  PORT=5050
  basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

class DevlopConfig(Config):
  DEBUG = True

class PrductConfig(Config):
  pass
