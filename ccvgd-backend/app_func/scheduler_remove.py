import datetime
import os
from datetime import datetime
import time

from apscheduler.schedulers.blocking import BlockingScheduler

def delete_files_jobs():
  path = os.path.abspath('.')

  # get modified time
  default_path = os.path.join(path,"single_csv")

  for item in os.listdir(default_path):
    current_path = os.path.join(default_path,item)
    modified_time = datetime.now() - datetime.fromtimestamp(os.path.getmtime(current_path))
    if modified_time.days>=1:
      os.remove(current_path)
scheduler = BlockingScheduler()
scheduler.add_job(delete_files_jobs, "cron", day_of_week="1-6", hour=23)
scheduler .start()


if __name__ == "__main__":
  deleteFiles()
