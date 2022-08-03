## To run scripts:
First you need to go into the directory ./ccvgd-backend/externalFile/pythonScript and find the readme file. Then follow the instruction below:
1. Create config.txt, put it directly under pythonScript directory.
2. Put all csv files under "All Datasets and Data Dictionary" directory.(Please make sure records are the most new version, and mistake records are fixed.)
3. Create empty directory "incorrect_records" under "pythonScript" to store incorrect records that can not be loaded into database.
4. Run ```main_process.py``` to process original csv files.
5. Run ```main_load.py``` to load processed csv files into database.

*config.txt:

```
{"user": "", "password": "", "host": "", "database": ""}
```
## Package version
1. python 3.7.3
2. mysql-connector-python 8.0.21
    * [Guide to install mysql-connector-python](https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html)
3. pandas 1.1.3

### Note
* Processed csv files are stored at "Database data" directory.
* Records can not be loaded into database are stored at "incorrect_records" directory.
* After running main_process.py, there may be some errors, so you need to fix them and then run main_process.py again.

