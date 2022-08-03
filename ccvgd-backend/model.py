
# import mysql.connector
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask import current_app
from manager import create_app
from config import DevlopConfig
#
# 1949,
#                                 1953,
#                                 1964,
#                                 1982,
#                                 1990,
#                                 2000,
#                                 2007,
#                                 1951
app = create_app(DevlopConfig)

db = SQLAlchemy(app)

# class BaseModel(object):
#   def add_update(self):
#     db.session.add(self)
#     db.session.commit()
#
#   def delete(self):
#     db.session.delete(self)
#     db.session.commit()
#
# class GazetteerInformation(db.Model):
#   __tablename__ = 'gazetteerinformation_村志信息'



