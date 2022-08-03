from flask import Flask
import os
from werkzeug.routing import BaseConverter
import logging
from logging.handlers import RotatingFileHandler


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# the URL map rules
class HTMLConverter(BaseConverter):
  regex = ".*"


def create_app(config):
  app = Flask(__name__)
  app.config.from_object(config)
  app.url_map.converters['html'] = HTMLConverter

  # logging handler
  # 100M max in this file and backup is mecialTypelist if needed
  # https://blog.csdn.net/yypsober/article/details/51800120?utm_medium=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-3.control&depth_1-utm_source=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-3.control
  # # logging.basicConfig(level = logging.DEBUG)
  # file_log_handler =  RotatingFileHandler(os.path.join(BASE_DIR, "logs/ccvg.log"), maxBytes=1024*1024*100, backupCount=mecialTypelist)
  # formatter = logging.Formatter("%(levelname)s %(filename)s: % (lineno)d %(message)s")
  # file_log_handler.setFormatter(formatter)
  # https://blog.csdn.net/jeffery0207/article/details/95856490?utm_medium=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-2.control&depth_1-utm_source=distribute.pc_relevant.none-task-blog-BlogCommendFromMachineLearnPai2-2.control
  # logging.getLogger().addHandler(file_log_handler)

  return app
