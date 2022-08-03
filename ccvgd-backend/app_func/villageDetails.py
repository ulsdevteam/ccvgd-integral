# coding=utf-8

import csv
import json
import os
import re

import mysql.connector
from flask import Blueprint, jsonify, request, session, send_from_directory
from flask_cors import CORS
from status_code import *
from config import *

village_blueprint = Blueprint('village', __name__)
CORS(village_blueprint)

# test
@village_blueprint.route("/search", methods=["POST"], strict_slashes=False)
def getAll():
  data = request.get_data()
  json_data = json.loads(data.decode("utf-8"))
  village_id = json_data.get("villageid")
  topic = json_data.get("topic")

  topics = ["village", "gazetteerinformation", "naturaldisasters", "naturalenvironment", "military", "education",
            "economy", "familyplanning",
            "population", "ethnicgroups", "fourthlastNames", "firstavailabilityorpurchase"]

  if topic not in topics:
    return "Topic not in the 12 tables"

  # Get connection
  # mydb = mysql.connector.connect(
  #   host="localhost",
  #   user="root",
  #   password="123456",
  #   port=3306,
  #   database="CCVG")

  mydb = mysql.connector.connect(
    host=mysql_host,
    user=mysql_username,
    password=mysql_password,
    port=mysql_port,
    database=mysql_database)


  mycursor = mydb.cursor()

  # Get gazetteerName
  mycursor.execute(
    "SELECT gazetteerTitle_村志书名 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(village_id))
  gazetteerName = mycursor.fetchone()[0]

  if topic == "village":
    # table1 村庄信息
    table = {}
    table["field"] = ["gazetteerId", "gazetteerName", "villageId", "villageName", "province", "city", "county",
                      "category1",
                      "data", "unit"]
    table["data"] = []
    table["filter"] = []
    filters = {}

    mycursor.execute("SELECT name_名称 FROM villageGeographyCategory_村庄地理类")
    geographyItems = mycursor.fetchall()
    for geography in geographyItems:
      filters[geography[0]] = 0

    table["filter"].append(filters)

    # Get province county villageName city
    mycursor.execute(
      "SELECT p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字, v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码={};".format(
        village_id))
    allNames = mycursor.fetchone()
    province = allNames[0]
    city = allNames[1]
    county = allNames[2]
    villageName = allNames[3]

    mycursor.execute(
      "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.gazetteerId_村志代码={} AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码".format(
        village_id))
    geographyList = mycursor.fetchall()
    for item in geographyList:
      d = {}
      d["gazetteerId"] = village_id
      d["gazetteerName"] = gazetteerName
      d["villageId"] = village_id
      d["villageName"] = villageName
      d["province"] = province
      d["county"] = county
      d["city"] = city
      d["category1"] = item[1]
      table["filter"][0][item[1]] += 1
      d["data"] = item[0]
      d["unit"] = item[2]
      table["data"].append(d)

  elif topic == "gazetteerinformation":
    # table2 村志信息
    table = {}
    table["field"] = ["villageId", "villageName", "gazetteerId", "gazetteerName", "publishYear", "publishType"],
    table["data"] = []

    mycursor.execute(
      "SELECT yearOfPublication_出版年, publicationType_出版类型 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(
        village_id))
    publicationList = mycursor.fetchall()

    mycursor.execute("SELECT nameChineseCharacters_村名汉字 FROM village_村 WHERE gazetteerId_村志代码={}".format(village_id))
    name = mycursor.fetchone()[0]
    for item in publicationList:
      d = {}
      d["villageId"] = village_id
      d["villageName"] = name
      d["gazetteerId"] = village_id
      d["gazetteerName"] = gazetteerName
      d["publishYear"] = item[0]
      d["publishType"] = item[1]
      table["data"].append(d)

  elif topic == "naturaldisasters":
    # table3 自然灾害
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "year", "category1"],
    table["data"] = []
    table["filter"] = []
    mycursor.execute("SELECT name_名称 FROM naturalDisastersCategory_自然灾害类")
    disasterItem = mycursor.fetchall()

    filters = {}
    for disaster in disasterItem:
      filters[disaster[0]] = 0

    table["filter"].append(filters)

    mycursor.execute(
      "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a, naturalDisastersCategory_自然灾害类 as b WHERE gazetteerId_村志代码={} AND a.categoryId_类别代码=b.categoryId_类别代码".format(
        village_id))
    disasterList = mycursor.fetchall()

    for item in disasterList:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["year"] = item[1]
      d["category1"] = item[0]
      table["filter"][0][item[0]] += 1
      table["data"].append(d)

  elif topic == "naturalenvironment":
    # table4 自然环境
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category1", "data", "unit"]
    table["data"] = []
    table["filter"] = []

    filters = {}
    mycursor.execute("SELECT name_名称 FROM naturalEnvironmentCategory_自然环境类")
    naturalenvironmentItem = mycursor.fetchall()
    for environment in naturalenvironmentItem:
      filters[environment[0]] = 0
    table["filter"].append(filters)

    mycursor.execute(
      "SELECT a.data_数据, b.name_名称, c.name_名称 FROM naturalEnvironment_自然环境 as a, naturalEnvironmentCategory_自然环境类 as b,naturalEnvironmentUnit_自然环境单位 as c \
      WHERE gazetteerId_村志代码={} AND a.categoryId_类别代码=b.categoryId_类别代码 \
      AND a.unitId_单位代码=c.unitId_单位代码".format(village_id))

    naturalList = mycursor.fetchall()

    for item in naturalList:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["data"] = item[0]
      table["filter"][0][item[1]] += 1
      d["category1"] = item[1]
      d["unit"] = item[2]
      table["data"].append(d)

  elif topic == "military":
    # table5 军事政治
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear", "data", "unit"]
    table["data"] = []
    table["filter_2"] = []
    category_map = {
      2: "共产党员 CCP Membership",
      7: "阶级成分 Class Status",
      13: "新党员 New CCP Membership"
    }

    filters = {}
    mycursor.execute("SELECT categoryId_类别代码, name_名称 FROM militarycategory_军事类")
    militaryItem = mycursor.fetchall()
    for military in militaryItem:
      # 得到父类
      if military[0] in [2, 7, 13]:
        filters[military[1]] = {}

      # 得到子类
      else:
        mycursor.execute(
          "SELECT name_名称,parentId_父类代码 FROM militarycategory_军事类 WHERE  categoryId_类别代码 = {}".format(military[0]))
        name, parentId = mycursor.fetchall()[0]
        parent_name = ""
        for index in category_map:
          if index == parentId:
            parent_name = category_map[index]
        if parent_name != "":
          filters[parent_name][name] = 0
        else:
          filters[name] = 0

    table["filter_2"].append(filters)

    mycursor.execute(
      "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
      m.endYear_结束年, data_数据, mu.name_名称\
   FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
   JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
   WHERE gazetteerId_村志代码={}".format(village_id))
    militraryList = mycursor.fetchall()

    for i, item in enumerate(militraryList):
      d = {}
      if item[1] != None:
        mycursor.execute("SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
        parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
        d["category1"] = parent
        table["filter_2"][0][parent][item[2]] += 1
        d["category2"] = item[2]

      else:
        d["category1"] = item[2]
        table["filter_2"][0][item[2]] += 1
        d["category2"] = "null"  # 没有父类代码 说明本身就是父类
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id

      d["startYear"] = item[3]
      d["endYear"] = item[4]
      d["data"] = item[5]
      d["unit"] = item[6]
      table["data"].append(d)

  elif topic == "education":
    # table6 教育
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear",
                      "data",
                      "unit"]
    table["data"] = []
    table["filter_2"] = []
    category_map = {1: "受教育程度 Highest Level of Education"}

    filters = {}
    mycursor.execute("SELECT categoryId_类别代码, name_名称 FROM educationCategory_教育类")
    educationItem = mycursor.fetchall()
    for education in educationItem:
      if education[0] == 1:
        filters[education[1]] = {}
      else:
        mycursor.execute(
          "SELECT name_名称,parentId_父类代码 FROM educationCategory_教育类 WHERE  categoryId_类别代码 = {}".format(education[0]))
        name, parentId = mycursor.fetchall()[0]
        parent_name = ""
        for index in category_map:
          if index == parentId:
            parent_name = category_map[index]
        if parent_name != "":
          filters[parent_name][name] = 0
        else:
          filters[name] = 0

    table["filter_2"].append(filters)

    mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
      ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
      ON e.unitId_单位代码=eu.unitId_单位代码 \
      WHERE e.gazetteerId_村志代码={}".format(int(village_id)))

    educationList = mycursor.fetchall()

    for item in educationList:
      d = {}
      if item[1] != None:
        d["category1"] = "受教育程度 Highest Level of Education"
        d["category2"] = item[2]
        table["filter_2"][0]["受教育程度 Highest Level of Education"][item[2]] += 1
        # table["filter"][0][item[2]] += 1
      else:
        d["category1"] = item[2]
        table["filter_2"][0][item[2]] += 1
        d["category2"] = "null"
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id

      # d["category1"] = item[2]
      d["startYear"] = item[3]
      d["endYear"] = item[4]
      d["data"] = item[5]
      d["unit"] = item[6]
      table["data"].append(d)

  elif topic == "economy":
    # table7 经济
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "category3", "startYear", "endYear",
                      "data",
                      "unit"]
    table["data"] = []
    table["filter_2"] = []
    category_map = {
      1: "总产值 Gross Output Value", 19: "集体经济收入 Collective Economic Income", 41: "电价 Electricity Price",
      47: "用电量 Electricity Consumption", 56: "水价 Water Price", 62: "用水量 Water Consumption"
    }

    filters = {}
    mycursor.execute("SELECT categoryId_类别代码, name_名称 FROM economyCategory_经济类")
    economyItem = mycursor.fetchall()
    for economy in economyItem:
      if economy[0] in [1, 19, 41, 47, 56, 62]:
        filters[economy[1]] = {}
      else:
        mycursor.execute(
          "SELECT name_名称,parentId_父类代码 FROM economyCategory_经济类 WHERE  categoryId_类别代码 = {}".format(economy[0]))
        name, parentId = mycursor.fetchall()[0]
        parent_name = ""
        for index in category_map:
          if index == parentId:
            parent_name = category_map[index]
        if parent_name != "":
          filters[parent_name][name] = 0
        else:
          filters[name] = 0
    table["filter_2"].append(filters)

    mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
    e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
    ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
    ON e.unitId_单位代码=eu.unitId_单位代码 \
    WHERE e.gazetteerId_村志代码 ={}".format(village_id))
    econmialList = mycursor.fetchall()
    for item in econmialList:
      d = {}
      # the category2 also has two upper layer
      if item[1] not in [1, 19, 37, 38, 39, 40, 41, 47, 56, 62] and item[1] != None:
        d["category3"] = item[2]
        mycursor.execute(
          "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
        categoryList = mycursor.fetchone()
        categoryId, d["category2"] = categoryList[0], categoryList[1]

        mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
        d["category1"] = mycursor.fetchone()[0]

        table["filter_2"][0][d["category1"]][d["category2"]] += 1
        print(d["category1"], d["category2"])
        # table["filter"][0][d["category1"]] += 1
        # table["filter"][0][d["category2"]] += 1

      # the category2 has no upper layer
      elif item[1] == None:
        d["category1"] = item[2]
        d["category3"] = "null"
        d["category2"] = "null"
        table["filter_2"][0][item[2]] += 1


      # the category2 has onw upper layers
      else:
        d["category2"] = item[2]
        mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
        d["category1"] = mycursor.fetchone()[0]
        d["category3"] = "null"
        table["filter_2"][0][d["category1"]][item[2]] += 1

      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["startYear"] = item[3]
      d["endYear"] = item[4]
      d["data"] = item[5]
      d["unit"] = item[6]
      table["data"].append(d)

  elif topic == "familyplanning":
    # table8 计划生育
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category", "startYear", "endYear", "data", "unit"]
    table["data"] = []
    table["filter"] = []

    filters = {}
    mycursor.execute("SELECT name_名称 FROM familyplanningcategory_计划生育类")
    familyplanningItem = mycursor.fetchall()
    for familyplanning in familyplanningItem:
      filters[familyplanning[0]] = 0
    table["filter"].append(filters)

    mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
    f.endYear_结束年,f.data_数据 ,fu.name_名称 \
    FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
    ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
    ON f.unitId_单位代码=fu.unitId_单位代码 \
    WHERE f.gazetteerId_村志代码 ={}".format(village_id))

    familyplanningList = mycursor.fetchall()

    for item in familyplanningList:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["category"] = item[0]
      table["filter"][0][item[0]] += 1
      d["startYear"] = item[1]
      d["endYear"] = item[2]
      d["data"] = item[3]
      d["unit"] = item[4]
      table["data"].append(d)

  elif topic == "population":

    # table9 人口
    table = {}
    table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table["data"] = []
    table["filter_2"] = []
    category_map = {1: "人口 Population", 5: "农转非 Agricultural to Non-Agricultural Hukou / Change of Residency Status",
                    13: "残疾人数 Disabled Population",
                    25: "迁入 Migration In",
                    29: "迁出 Migration Out"}

    filters = {}
    mycursor.execute("SELECT categoryId_类别代码, name_名称 FROM populationcategory_人口类")
    populationItem = mycursor.fetchall()
    for population in populationItem:
      if population[0] in [1, 5, 13, 25, 29]:
        filters[population[1]] = {}
      else:
        mycursor.execute(
          "SELECT name_名称,parentId_父类代码 FROM populationcategory_人口类 WHERE  categoryId_类别代码 = {}".format(population[0]))
        name, parentId = mycursor.fetchall()[0]
        parent_name = ""
        for index in category_map:
          if index == parentId:
            parent_name = category_map[index]
        if parent_name != "":
          filters[parent_name][name] = 0
        else:
          filters[name] = 0
    table["filter_2"].append(filters)

    mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
  p.endYear_结束年,p.data_数据 ,pu.name_名称 \
  FROM population_人口 p JOIN populationcategory_人口类 pc \
  ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
  ON p.unitId_单位代码=pu.unitId_单位代码 \
  WHERE p.gazetteerId_村志代码 ={}".format(village_id))

    populationList = mycursor.fetchall()

    for item in populationList:
      d = {}
      if item[1] != None:
        mycursor.execute("SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
        d["category1"] = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
        d["category2"] = item[2]
        table["filter_2"][0][d["category1"]][item[2]] += 1
        # table["filter_2"][0][item[2]] += 1
      else:
        d["category1"] = item[2]
        table["filter_2"][0][item[2]] += 1
        d["category2"] = "null"  # 没有父类代码 说明本身就是父类

      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["startYear"] = item[3]
      d["endYear"] = item[4]
      d["data"] = item[5]
      d["unit"] = item[6]
      table["data"].append(d)

  elif topic == "ethnicgroups":
    # table10 民族
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category1", "startYear", "endYear", "data", "unit"]
    table["data"] = []
    table["filter"] = []

    filters = {}
    mycursor.execute("SELECT name_名称 FROM ethnicGroupsCategory_民族类")
    ethnicgroupsItem = mycursor.fetchall()
    for ethnicgroups in ethnicgroupsItem:
      filters[ethnicgroups[0]] = 0
    table["filter"].append(filters)

    mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
    eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
    FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
    ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
    ON eth.unitId_单位代码=ethu.unitId_单位代码 \
    WHERE eth.gazetteerId_村志代码 ={};".format(village_id))

    ethnicgroupList = mycursor.fetchall()
    for item in ethnicgroupList:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["category1"] = item[0]
      table["filter"][0][item[0]] += 1
      d["startYear"] = item[1]
      d["endYear"] = item[2]
      d["data"] = item[3]
      d["unit"] = item[4]
      table["data"].append(d)

  elif topic == "fourthlastNames":
    # table11 姓氏
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "firstLastNameId", "secondLastNameId", "thirdLastNameId",
                      "fourthLastNameId",
                      "fifthLastNameId", "totalNumberOfLastNameInVillage"]
    table["data"] = []

    mycursor.execute("SELECT firstlastNamesId_姓氏代码, secondlastNamesId_姓氏代码, thirdlastNamesId_姓氏代码, \
      fourthlastNamesId_姓氏代码, fifthlastNamesId_姓氏代码, totalNumberofLastNamesinVillage_姓氏总数 \
      FROM lastName_姓氏 WHERE gazetteerId_村志代码={}".format(village_id))
    nameList = mycursor.fetchall()
    if nameList!=[]:
      l = []
      for z in range(len(nameList[0]) - 1):
        if nameList[0][z] == None:
          l.append("")
        else:
          mycursor.execute(
            "SELECT nameChineseCharacters_姓氏汉字 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 ={}".format(
              nameList[0][z]))
          l.append(mycursor.fetchone()[0])


      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["firstLastNameId"] = l[0]
      d["secondLastNameId"] = l[1]
      d["thirdLastNameId"] = l[2]
      d["fourthLastNameId"] = l[3]
      d["fifthLastNameId"] = l[4]
      d["totalNumberOfLastNameInVillage"] = nameList[0][-1]
    else:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["firstLastNameId"] = None
      d["secondLastNameId"] = None
      d["thirdLastNameId"] = None
      d["fourthLastNameId"] = None
      d["fifthLastNameId"] = None
      d["totalNumberOfLastNameInVillage"] = None

    table["data"].append(d)

  elif topic == "firstavailabilityorpurchase":
    # table12 第一次拥有或购买年份
    table = {}
    table["field"] = ["gazetteerName", "gazetteerId", "category", "year"]
    table["data"] = []
    table["filter"] = []

    filters = {}
    mycursor.execute("SELECT name_名称 FROM firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类")
    firstavailabilityItem = mycursor.fetchall()
    for firstavailability in firstavailabilityItem:
      filters[firstavailability[0]] = 0
    table["filter"].append(filters)
    mycursor.execute("SELECT f.year_年份,fc.name_名称 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 f JOIN firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类 fc \
       ON f.categoryId_类别代码=fc.categoryId_类别代码 WHERE f.gazetteerId_村志代码={}".format(village_id))

    firstList = mycursor.fetchall()

    for i in firstList:
      d = {}
      d["gazetteerName"] = gazetteerName
      d["gazetteerId"] = village_id
      d["category"] = i[0] if i[0] != None else None
      if i[0] != None:
        table["filter"][0][i[0]] += 1
      d["year"] = i[1] if i[1] != None else None
      table["data"].append(d)

  # check validation
  count_valid = 0

  # check filter item
  if "filter" in table.keys():
    for j in list(table["filter"][0].keys()):
      if table["filter"][0][j] == 0:
        del table["filter"][0][j]
        continue

  # check filter with 2 category item
  if "filter_2" in table.keys():
    for item in list(table["filter_2"][0].keys()):
      if table["filter_2"][0][item] == 0:
        del table["filter_2"][0][item]
      elif type(table["filter_2"][0][item]) != int:
        for less_item in list(table["filter_2"][0][item]):
          if table["filter_2"][0][item][less_item] == 0:
            del table["filter_2"][0][item][less_item]
            continue
    for item in list(table["filter_2"][0].keys()):
      if table["filter_2"][0][item] == {}:
        del table["filter_2"][0][item]
        continue

  # check data
  for i in table["data"]:
    check_value = i.values()
    if check_value == None:
      count_valid += 1
  if count_valid == len(table["data"]):
    table["isEmptyTable"] = "True"

    return jsonify({"code":4002, "message":"This table is empty! Please try again"})
  else:
    table["isEmptyTable"] = "False"
    # write into csv
    path = os.getcwd()
    f = open(os.path.join(path,"app_func","single_csv","{}_{}.csv".format(village_id, topic)), 'w', encoding='utf-8')
    # f = open('/home/yuelv/CCVG/app_func/single_csv/{}_{}.csv'.format(village_id, topic), 'w', encoding='utf-8')


    csv_writer = csv.writer(f)
    title = [i for i in table["field"]]
    if len(title)==1:
      title = title[0]
    csv_writer.writerow(title)
    for item in table["data"]:
      temp_l = []
      for ti in title:
        temp_l.append(item[ti])
      csv_writer.writerow(temp_l)

    return jsonify(table)

#
# @village_blueprint.route("/namesearch", methods=["GET", "POST"], strict_slashes=False)
# def getByName():
#   table = {}
#   table["data"] = []
# 
#   # mydb = mysql.connector.connect(
#   #   host="localhost",
#   #   user="root",
#   #   password="123456",
#   #   port=3306,
#   #   database="CCVG")
#   mydb = mysql.connector.connect(
#     host=mysql_host,
#     user=mysql_username,
#     password=mysql_password,
#     port=mysql_port,
#     database=mysql_database)
# 
#   mycursor = mydb.cursor()
# 
#   if request.method == "GET":
#     mycursor.execute("SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.gazetteerId_村志代码 \
#      FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
#     JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.gazetteerId_村志代码= jointable.gazetteerId_村志代码 LIMIT 100;")
#     nameList = mycursor.fetchall()
# 
#   else:
# 
#     data = request.get_data()
#     json_data = json.loads(data.decode("utf-8"))
#     namefilter = json_data.get("namefilter")
#     if len(namefilter) == 0 or namefilter == " ":
#       return "No paramters in namefilter"
# 
#     else:
#       name_check = re.compile(r'[A-Za-z]', re.S)
#       if len(re.findall(name_check,namefilter)):
#         print("this is english mode")
#         mycursor.execute("SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.gazetteerId_村志代码 \
#              FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
#             JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.gazetteerId_村志代码= jointable.gazetteerId_村志代码 WHERE village.nameHanyuPinyin_村名汉语拼音 LIKE '%{}%';".format(
#           namefilter))
#       else:
#         print("this is Chinese mode")
#         mycursor.execute("SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.gazetteerId_村志代码 \
#                      FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
#                     JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.gazetteerId_村志代码= jointable.gazetteerId_村志代码 WHERE village.nameChineseCharacters_村名汉字 LIKE '%{}%';".format(
#           namefilter))
# 
#       nameList = mycursor.fetchall()
#       print(nameList)
# 
#   for item in nameList:
#     temp = {}
#     temp["name"] = item[0]
#     temp["county"] = item[1]
#     temp["city"] = item[2]
#     temp["province"] = item[3]
#     temp["id"] = str(item[4])
#     table["data"].append(temp)
# 
#   return jsonify(table)


@village_blueprint.route("/namesearch", methods=["GET", "POST"], strict_slashes=False)
def getByName():
    table = {}
    table["data"] = []
    conn = mysql.connector.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)
    mycursor = conn.cursor()

    if request.method == "GET":
        pageNumber = int(request.args.get("pageNumber"))
        sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
     FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
    JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 LIMIT {},{};".format(
            pageNumber*50, 50)

        mycursor.execute(sql)

        nameList = mycursor.fetchall()

    else:

        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        namefilter = json_data.get("namefilter")
        if len(namefilter) == 0 or namefilter == " ":
            return "No paramters in namefilter"

        else:
            name_check = re.compile(r'[A-Za-z]', re.S)
            if len(re.findall(name_check, namefilter)):
                print("this is english mode")
                mycursor.execute("SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
             FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
            JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE village.nameHanyuPinyin_村名汉语拼音 LIKE '%{}%';".format(
                    namefilter))
            else:
                print("this is Chinese mode")
                mycursor.execute("SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
                     FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
                    JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE village.nameChineseCharacters_村名汉字 LIKE '%{}%';".format(
                    namefilter))

            nameList = mycursor.fetchall()
            print(nameList)

    for item in nameList:
        temp = {}
        temp["name"] = item[0]
        temp["county"] = item[1]
        temp["city"] = item[2]
        temp["province"] = item[3]
        temp["id"] = str(int(float(item[4])))
        table["data"].append(temp)

    return jsonify(table)


# @village_blueprint.route("/advancesearch", methods=["POST"], strict_slashes=False)
# def advanceSearch():
#   data = request.get_data()
#   json_data = json.loads(data.decode("utf-8"))
#   villageid = json_data.get("villageid")
#   topic = json_data.get("topic")
#   year = json_data.get("year")
#   year_range = json_data.get("year_range")
#
#   # Get connection
#   mydb = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="123456",
#     port=3306,
#     database="CCVG")
#   mycursor = mydb.cursor()
#
#   topics = ["village", "gazetteerinformation", "naturaldisasters", "naturalenvironment", "military", "education",
#             "economy", "familyplanning",
#             "population", "ethnicgroups", "fourthlastNames", "firstavailabilityorpurchase"]
#   # get every topics' index in the
#   indexes = []
#   for i in topic:
#     if i not in topics:
#       return "topics is not fullfil requirement"
#     else:
#       indexes.append(topics.index(i) + 1)
#
#   dicts = {}
#   table = []
#
#   table1 = {}
#   table1["field"] = ["gazetteerId", "gazetteerName", "villageId", "villageName", "province", "city", "county",
#                      "category1",
#                      "data", "unit"]
#   table1["data"] = []
#   table1["tableNameChinese"] = "村庄基本信息"
#   dicts[1] = table1
#
#   table2 = {}
#   table2["field"] = ["villageId", "villageName", "gazetteerId", "gazetteerName", "publishYear", "publishType"]
#   table2["data"] = []
#   table2["tableNameChinese"] = "村志基本信息"
#   dicts[2] = table2
#
#   table3 = {}
#   table3["field"] = ["gazetteerName", "gazetteerId", "year", "category1"],
#   table3["data"] = []
#   table3["tableNameChinese"] = "自然灾害"
#   dicts[3] = table3
#
#   table4 = {}
#   table4["field"] = ["gazetteerName", "gazetteerId", "category1", "data", "unit"]
#   table4["data"] = []
#   table4["tableNameChinese"] = "自然环境"
#   dicts[4] = table4
#
#   table5 = {}
#   table5["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear", "data", "unit"]
#   table5["data"] = []
#   table5["tableNameChinese"] = "军事政治"
#   dicts[5] = table5
#
#   table6 = {}
#   table6["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear",
#                      "data",
#                      "unit"]
#   table6["data"] = []
#   table6["tableNameChinese"] = "教育"
#   dicts[6] = table6
#
#   table7 = {}
#   table7["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "category3", "startYear", "endYear",
#                      "data",
#                      "unit"]
#   table7["data"] = []
#   table7["tableNameChinese"] = "经济"
#   dicts[7] = table7
#
#   table8 = {}
#   table8["field"] = ["gazetteerName", "gazetteerId", "category", "startYear", "endYear", "data", "unit"]
#   table8["data"] = []
#   table8["tableNameChinese"] = "计划生育"
#   dicts[8] = table8
#
#   table9 = {}
#   table9["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
#   table9["data"] = []
#   table9["tableNameChinese"] = "人口"
#   dicts[9] = table9
#
#   table10 = {}
#   table10["field"] = ["gazetteerName", "gazetteerId", "category1", "startYear", "endYear", "data", "unit"]
#   table10["data"] = []
#   table10["tableNameChinese"] = "民族"
#   dicts[10] = table10
#
#   table11 = {}
#   table11["field"] = ["gazetteerName", "gazetteerId", "firstLastNameId", "secondLastNameId", "thirdLastNameId",
#                       "fourthLastNameId",
#                       "fifthLastNameId", "totalNumberOfLastNameInVillage"]
#   table11["data"] = []
#   table11["tableNameChinese"] = "姓氏"
#   dicts[11] = table11
#
#   table12 = {}
#   table12["field"] = ["gazetteerName", "gazetteerId", "category", "year"]
#   table12["data"] = []
#   table12["tableNameChinese"] = "第一次拥有或购买年份"
#   dicts[12] = table12
#
#   table2func = {}
#   table2func[3] = getNaturalDisaster
#   table2func[4] = getNaturalEnvironment
#   table2func[5] = getMilitary
#   table2func[6] = getEduaction
#   table2func[7] = getEconomy
#   table2func[8] = getFamilyPlanning
#   table2func[9] = getPopulation
#   table2func[10] = getEthnicgroups
#   table2func[11] = getFourthlastName
#   table2func[12] = getFirstAvailabilityorPurchase
#
#   # For every node in the village and we want to change
#   temp = {}
#   for village_id in villageid:
#     mycursor.execute(
#       "SELECT gazetteerTitle_村志书名 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(village_id))
#     gazetteerName = mycursor.fetchone()[0]
#
#     for i in getVillage(mycursor, village_id, gazetteerName)["data"]:
#       table1["data"].append(i)
#     for i in getGazetteer(mycursor, village_id, gazetteerName)["data"]:
#       table2["data"].append(i)
#
#     for index in indexes:
#       newTable = dicts[index]  # table3~12
#       temp[index] = newTable  # {3: table3...}
#       if index == 1 or index == 2:
#         continue
#       else:
#         func = table2func[index]
#         for j in func(mycursor, village_id, gazetteerName)["data"]:
#           newTable["data"].append(j)  # table3~12["data"].append(i) =>
#
#   table.append(table1)
#   table.append(table2)
#   for index in indexes:
#     if index == 1 or index == 2:
#       continue
#     else:
#       table.append(temp[index])
#   return jsonify(table)
#

@village_blueprint.route("/download/", methods=["GET"], strict_slashes=False)
def downloadData():
  dir_path = os.getcwd()
  single_dir = os.path.join(dir_path,"app_func","single_csv")
  # print("path is",os.path.join(single_dir, path))
  village_id = request.args.get("village")
  topic = request.args.get("topic", None)

  path = village_id+"_"+topic
  if os.path.exists(os.path.join(single_dir, path+".csv")):

    return send_from_directory(single_dir, path+".csv", as_attachment=True)
  return jsonify({"code":4003,"message":"File is not exist or file can't download"})


def getVillage(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  # Get province county villageName city
  mycursor.execute(
    "SELECT p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字, v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码={};".format(
      village_id))
  allNames = mycursor.fetchone()
  province = allNames[0]
  city = allNames[1]
  county = allNames[2]
  villageName = allNames[3]

  mycursor.execute(
    "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.gazetteerId_村志代码={} AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码".format(
      village_id))
  geographyList = mycursor.fetchall()
  for item in geographyList:
    d = {}
    d["gazetteerId"] = village_id
    d["gazetteerName"] = gazetteerName
    d["villageId"] = village_id
    d["villageName"] = villageName
    d["province"] = province
    d["county"] = county
    d["city"] = city
    d["category1"] = item[1]
    d["data"] = item[0]
    d["unit"] = item[2]
    table["data"].append(d)
  return table


def getGazetteer(mycursor, village_id, gazetteerName):
  mycursor.execute(
    "SELECT yearOfPublication_出版年, publicationType_出版类型 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(
      village_id))
  publicationList = mycursor.fetchall()
  table = {}
  table["data"] = []

  mycursor.execute("SELECT nameChineseCharacters_村名汉字 FROM village_村 WHERE gazetteerId_村志代码={}".format(village_id))
  name = mycursor.fetchone()[0]
  for item in publicationList:
    d = {}
    d["villageId"] = village_id
    d["villageName"] = name
    d["gazetteerId"] = village_id
    d["gazetteerName"] = gazetteerName
    d["publishYear"] = item[0]
    d["publishType"] = item[1]
    table["data"].append(d)
  return table


def getNaturalDisaster(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute(
    "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a, naturalDisastersCategory_自然灾害类 as b WHERE gazetteerId_村志代码={} AND a.categoryId_类别代码=b.categoryId_类别代码".format(
      village_id))
  disasterList = mycursor.fetchall()

  for item in disasterList:
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["year"] = item[1]
    d["category1"] = item[0]
    table["data"].append(d)
  return table


def getNaturalEnvironment(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute(
    "SELECT a.data_数据, b.name_名称, c.name_名称 FROM naturalEnvironment_自然环境 as a, naturalEnvironmentCategory_自然环境类 as b,naturalEnvironmentUnit_自然环境单位 as c \
    WHERE gazetteerId_村志代码={} AND a.categoryId_类别代码=b.categoryId_类别代码 \
    AND a.unitId_单位代码=c.unitId_单位代码".format(
      village_id))
  naturalList = mycursor.fetchall()

  for item in naturalList:
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category1"] = item[0]
    d["data"] = item[1]
    d["unit"] = item[2]
    table["data"].append(d)
  return table


def getMilitary(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute(
    "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
    m.endYear_结束年, data_数据, mu.name_名称\
 FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
 JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
 WHERE gazetteerId_村志代码={}".format(village_id))
  militraryList = mycursor.fetchall()

  parentIdList = []
  for i, item in enumerate(militraryList):
    if item[1] != None:
      mycursor.execute("SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
      parentIdList.append(mycursor.fetchone())
    else:
      parentIdList.append(["null"])
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category1"] = item[2]
    d["category2"] = parentIdList[-1][0]
    d["startYear"] = item[3]
    d["endYear"] = item[4]
    d["data"] = item[5]
    d["unit"] = item[6]
    table["data"].append(d)
  return table


def getEduaction(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
        ON e.unitId_单位代码=eu.unitId_单位代码 \
        WHERE e.gazetteerId_村志代码={}".format(int(village_id)))

  educationList = mycursor.fetchall()

  for item in educationList:
    d = {}
    if item[1] != None:
      d["category1"] = "受教育程度 Highest Level of Education"
      d["category2"] = item[2]
    else:
      d["category1"] = item[2]
      d["category2"] = "null"
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id

    # d["category1"] = item[2]
    d["startYear"] = item[3]
    d["endYear"] = item[4]
    d["data"] = item[5]
    d["unit"] = item[6]
    table["data"].append(d)
  return table


def getEconomy(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
      e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
      ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
      ON e.unitId_单位代码=eu.unitId_单位代码 \
      WHERE e.gazetteerId_村志代码 ={}".format(village_id))
  econmialList = mycursor.fetchall()
  for item in econmialList:
    d = {}
    # the category2 also has two upper layer
    if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
      d["category3"] = item[2]
      mycursor.execute(
        "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
      categoryList = mycursor.fetchone()
      categoryId, d["category2"] = categoryList[0], categoryList[1]

      mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
      d["category1"] = mycursor.fetchone()[0]

    # the category2 has no upper layer
    elif item[1] == None:
      d["category1"] = item[2]
      d["category3"] = "null"
      d["category2"] = "null"

    # the category2 has onw upper layers
    else:
      d["category2"] = item[2]
      mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
      d["category1"] = mycursor.fetchone()[0]
      d["category3"] = "null"

    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    # d["category1"] = item[2]
    d["startYear"] = item[3]
    d["endYear"] = item[4]
    d["data"] = item[5]
    d["unit"] = item[6]
    table["data"].append(d)
  return table


def getFamilyPlanning(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
      f.endYear_结束年,f.data_数据 ,fu.name_名称 \
      FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
      ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
      ON f.unitId_单位代码=fu.unitId_单位代码 \
      WHERE f.gazetteerId_村志代码 ={}".format(village_id))

  familyplanningList = mycursor.fetchall()

  for item in familyplanningList:
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category"] = item[0]
    d["startYear"] = item[1]
    d["endYear"] = item[2]
    d["data"] = item[3]
    d["unit"] = item[4]
    table["data"].append(d)
  return table


def getPopulation(mycursor, village_id, gazetteerName):
  table = {}
  table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
  table["data"] = []
  table["tableNameChinese"] = "人口"
  mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
    p.endYear_结束年,p.data_数据 ,pu.name_名称 \
    FROM population_人口 p JOIN populationcategory_人口类 pc \
    ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
    ON p.unitId_单位代码=pu.unitId_单位代码 \
    WHERE p.gazetteerId_村志代码 ={}".format(village_id))

  populationList = mycursor.fetchall()

  for item in populationList:
    d = {}
    if item[1] != None:
      mycursor.execute("SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
      d["category2"] = mycursor.fetchone()[0]
    else:
      d["category2"] = "null"

    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category1"] = item[2]
    d["startYear"] = item[3]
    d["endYear"] = item[4]
    d["data"] = item[5]
    d["unit"] = item[6]
    table["data"].append(d)
  return table


def getEthnicgroups(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
      eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
      FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
      ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
      ON eth.unitId_单位代码=ethu.unitId_单位代码 \
      WHERE eth.gazetteerId_村志代码 ={};".format(village_id))

  ethnicgroupList = mycursor.fetchall()
  for item in ethnicgroupList:
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category1"] = item[0]
    d["startYear"] = item[1]
    d["endYear"] = item[2]
    d["data"] = item[3]
    d["unit"] = item[4]
    table["data"].append(d)
  return table


def getFourthlastName(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT firstlastNamesId_姓氏代码, secondlastNamesId_姓氏代码, thirdlastNamesId_姓氏代码, \
        fourthlastNamesId_姓氏代码, fifthlastNamesId_姓氏代码, totalNumberofLastNamesinVillage_姓氏总数 \
        FROM lastName_姓氏 WHERE gazetteerId_村志代码={}".format(village_id))
  nameList = mycursor.fetchall()

  l = []
  for z in range(len(nameList[0]) - 1):
    if nameList[0][z] == None:
      l.append("")
    else:
      mycursor.execute(
        "SELECT nameChineseCharacters_姓氏汉字 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 ={}".format(
          nameList[0][z]))
      l.append(mycursor.fetchone()[0])

  d = {}
  d["gazetteerName"] = gazetteerName
  d["gazetteerId"] = village_id
  d["firstLastNameId"] = l[0]
  d["secondLastNameId"] = l[1]
  d["thirdLastNameId"] = l[2]
  d["fourthLastNameId"] = l[3]
  d["fifthlastNamesId_姓氏代码"] = l[4]
  d["totalNumberOfLastNameInVillage"] = nameList[0][-1]

  table["data"].append(d)
  return table


def getFirstAvailabilityorPurchase(mycursor, village_id, gazetteerName):
  table = {}
  table["data"] = []
  mycursor.execute("SELECT f.year_年份,fc.name_名称 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 f JOIN firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类 fc \
         ON f.categoryId_类别代码=fc.categoryId_类别代码 WHERE f.gazetteerId_村志代码={}".format(village_id))

  firstList = mycursor.fetchall()

  for i in firstList:
    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["category"] = i[0] if i[0] != None else None
    d["year"] = i[1] if i[1] != None else None
    table["data"].append(d)

  return table
