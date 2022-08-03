import csv
import json
import os
import re

import mysql.connector
from flask import Blueprint, jsonify, request, session, send_from_directory
from flask_cors import CORS
from status_code import *
from config import *


def get_dicts():
    # Get connection
    dicts = {}
    # 自然灾害
    table3 = {}
    table3["field"] = ["villageId","gazetteerName", "gazetteerId", "year", "category1"]
    table3["data"] = []
    table3["year"] = []
    table3["tableNameChinese"] = "自然灾害"
    dicts[3] = table3

    table4 = {}
    table4["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "data", "unit"]
    table4["data"] = []
    table4["tableNameChinese"] = "自然环境"
    dicts[4] = table4

    # 军事
    table5 = {}
    table5["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear", "data", "unit"]
    table5["data"] = []
    table5["year"] = []
    table5["tableNameChinese"] = "军事政治"
    dicts[5] = table5

    # 教育
    table6 = {}
    table6["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear",
                       "data",
                       "unit"]
    table6["data"] = []
    table6["tableNameChinese"] = "教育"
    table6["year"] = []
    dicts[6] = table6

    # 经济
    table7 = {}
    table7["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "category2", "category3", "startYear", "endYear",
                       "data",
                       "unit"]
    table7["data"] = []
    table7["year"] = []
    table7["tableNameChinese"] = "经济原始数据"
    dicts[7] = table7

    # 计划生育
    table8 = {}
    table8["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "startYear", "endYear", "data", "unit"]
    table8["data"] = []
    table8["year"] = []
    table8["tableNameChinese"] = "计划生育"
    dicts[8] = table8

    # 人口
    table9 = {}
    table9["field"] = ["villageId",'gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table9["data"] = []
    table9["year"] = []
    table9["tableNameChinese"] = "人口"
    dicts[9] = table9

    # 民族
    table10 = {}
    table10["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "startYear", "endYear", "data", "unit"]
    table10["data"] = []
    table10["year"] = []
    table10["tableNameChinese"] = "民族"
    dicts[10] = table10

    table11 = {}
    table11["field"] = ["villageId","gazetteerName", "gazetteerId", "firstLastNameId", "secondLastNameId", "thirdLastNameId",
                        "fourthLastNameId",
                        "fifthLastNameId", "totalNumberOfLastNameInVillage"]
    table11["data"] = []
    table11["tableNameChinese"] = "姓氏"
    dicts[11] = table11

    # 第一次购买年份
    table12 = {}
    table12["field"] = ["villageId","gazetteerName", "gazetteerId", "category1", "year"]
    table12["data"] = []
    table12["year"] = []
    table12["tableNameChinese"] = "第一次拥有或购买年份"
    dicts[12] = table12

    # new test
    table13 = {}
    table13["field"] = ["villageId", "gazetteerName", "gazetteerId", "category1", "category2", "category3", "startYear",
                        "endYear",
                        "data",
                        "unit"]
    table13["data"] = []
    table13["year"] = []
    table13["tableNameChinese"] = "统一单位经济"
    dicts[13] = table13

    return dicts



