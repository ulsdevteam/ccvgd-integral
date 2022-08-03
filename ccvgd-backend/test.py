import datetime
import os
from datetime import datetime
import time

from apscheduler.schedulers.blocking import BlockingScheduler


def TimeStampToTime(timestamp):
  timeStruct = time.localtime(timestamp)
  return time.strftime('%Y-%m-%d %H:%M:%S', timeStruct)


def get_FileCreateTime(filePath):
  filePath = filePath, 'utf8'
  t = os.path.getctime(filePath)
  return TimeStampToTime(t)


def get_FileModifyTime(filePath):
  filePath = filePath, 'utf8'
  t = os.path.getmtime(filePath)
  return TimeStampToTime(t)


def deleteFiles():
  path = "/home/yuelv/CCVG/app_func/single_csv"
  print("begin path",path)

  for item in os.listdir(path):
    file_path = os.path.join(path, item)
    print(file_path)

    # get_FileCreateTime(item)
  # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# BlockingScheduler
# scheduler = BlockingScheduler()
# scheduler.add_job(deleteFiles, 'cron', day_of_week='1-5', hour=6, minute=30)
# scheduler.start()

if __name__ == "__main__":
  deleteFiles()
