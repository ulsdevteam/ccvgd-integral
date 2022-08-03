import csv
import json
import os
import re

import mysql.connector
from flask import Blueprint, jsonify, request, session, send_from_directory
from flask_cors import CORS
from status_code import *
from config import *
from .utils import get_dicts

en_blueprint = Blueprint('en', __name__)
CORS(en_blueprint)


@en_blueprint.route("/search", methods=["POST"], strict_slashes=False)
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

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_username,
        password=mysql_password,
        port=mysql_port,
        database=mysql_database)


    mycursor = mydb.cursor()

    # Get gazetteerName
    mycursor.execute(
        "SELECT gazetteerTitleHanyuPinyin_村志书名汉语拼音 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(
            village_id))
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
            "SELECT p.nameHanyuPinyin_省汉语拼音, ci.nameHanyuPinyin_市汉语拼音 , co.nameHanyuPinyin_县或区汉语拼音, v.nameHanyuPinyin_村名汉语拼音  FROM villageCountyCityProvince_村县市省 vccp JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码={};".format(
                village_id))
        allNames = mycursor.fetchone()
        province = allNames[0]
        city = allNames[1]
        county = allNames[2]
        villageName = allNames[3]

        mycursor.execute(
            "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.villageInnerId_村庄内部代码={} AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码".format(
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

        mycursor.execute("SELECT nameHanyuPinyin_村名汉语拼音 FROM village_村 WHERE gazetteerId_村志代码={}".format(village_id))
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
            "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a, naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} AND a.categoryId_类别代码=b.categoryId_类别代码".format(
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
            WHERE villageInnerId_村庄内部代码={} AND a.categoryId_类别代码=b.categoryId_类别代码 \
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
        table["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear", "data",
                          "unit"]
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
                    "SELECT name_名称,parentId_父类代码 FROM militarycategory_军事类 WHERE  categoryId_类别代码 = {}".format(
                        military[0]))
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
                    "SELECT name_名称,parentId_父类代码 FROM educationCategory_教育类 WHERE  categoryId_类别代码 = {}".format(
                        education[0]))
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
                    "SELECT name_名称,parentId_父类代码 FROM economyCategory_经济类 WHERE  categoryId_类别代码 = {}".format(
                        economy[0]))
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
        table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data',
                          'unit']
        table["data"] = []
        table["filter_2"] = []
        category_map = {1: "人口 Population",
                        5: "农转非 Agricultural to Non-Agricultural Hukou / Change of Residency Status",
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
                    "SELECT name_名称,parentId_父类代码 FROM populationcategory_人口类 WHERE  categoryId_类别代码 = {}".format(
                        population[0]))
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
        if nameList != []:
            l = []
            for z in range(len(nameList[0]) - 1):
                if nameList[0][z] == None:
                    l.append("")
                else:
                    mycursor.execute(
                        "SELECT nameHanyuPinyin_姓氏汉语拼音 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 ={}".format(
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

        return "This table is empty!"
    else:
        table["isEmptyTable"] = "False"
        # write into csv
        f = open('/home/yuelv/CCVG/app_func/single_csv/{}_{}'.format(village_id, topic), 'w', encoding='utf-8')
        # need to modify
        # f = open('/home/yuelv/CCVG/app_func/single_csv/{}_{}.csv'.format(village_id, topic), 'w', encoding='utf-8')
        csv_writer = csv.writer(f)
        title = [i for i in table["field"]]
        if len(title) == 1:
            title = title[0]
        csv_writer.writerow(title)
        for item in table["data"]:
            temp_l = []
            for ti in title:
                temp_l.append(item[ti])
            csv_writer.writerow(temp_l)

        return jsonify(table)


@en_blueprint.route("/namesearch", methods=["POST", "GET"], strict_slashes=False)
def getByName():
    table = {}
    table["data"] = []

    # mydb = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     password="123456",
    #     port=3306,
    #     database="CCVG")

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_username,
        password=mysql_password,
        port=mysql_port,
        database=mysql_database)

    mycursor = mydb.cursor()

    if request.method == "GET":
        pageNumber = int(request.args.get("pageNumber"))
    #     mycursor.execute("SELECT village.nameHanyuPinyin_村名汉语拼音, county.nameHanyuPinyin_县或区汉语拼音, city.nameHanyuPinyin_市汉语拼音, province.nameHanyuPinyin_省汉语拼音, jointable.villageInnerId_村庄内部代码 \
    #  FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
    # JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 LIMIT 100;")
    #     nameList = mycursor.fetchall()
        sql = "SELECT village.nameHanyuPinyin_村名汉语拼音, county.nameHanyuPinyin_县或区汉语拼音, city.nameHanyuPinyin_市汉语拼音, province.nameHanyuPinyin_省汉语拼音, jointable.villageInnerId_村庄内部代码 \
     FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
    JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 LIMIT {},{};".format(
            pageNumber * 50, 50)
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
                mycursor.execute("SELECT village.nameHanyuPinyin_村名汉语拼音, county.nameHanyuPinyin_县或区汉语拼音, city.nameHanyuPinyin_市汉语拼音, province.nameHanyuPinyin_省汉语拼音, jointable.villageInnerId_村庄内部代码 \
             FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
            JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE village.nameHanyuPinyin_村名汉语拼音 LIKE '%{}%';".format(
                    namefilter))
            else:
                print("this is Chinese mode")
                mycursor.execute("SELECT village.nameHanyuPinyin_村名汉语拼音, county.nameHanyuPinyin_县或区汉语拼音, city.nameHanyuPinyin_市汉语拼音, province.nameHanyuPinyin_省汉语拼音, jointable.villageInnerId_村庄内部代码 \
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
        temp["id"] = item[4]
        table["data"].append(temp)

    return jsonify(table)


@en_blueprint.route("/advancesearch", methods=["POST"], strict_slashes=False)
def en_advanceSearch():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    villageid = json_data.get("villageid")
    topic = json_data.get("topic")
    year = json_data.get("year", None)
    year_range = json_data.get("year_range", None)

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_username,
        password=mysql_password,
        port=mysql_port,
        database=mysql_database)
    mycursor = mydb.cursor()

    topics = ["village", "gazetteerinformation", "naturaldisasters", "naturalenvironment", "military", "education",
              "economy", "familyplanning",
              "population", "ethnicgroups", "fourthlastNames", "firstavailabilityorpurchase"]
    # get every topics' index in the
    indexes = []
    for i in topic:
        if i not in topics:
            return "topics is not fullfil requirement"
        else:
            indexes.append(topics.index(i) + 1)

    # dicts = {}
    table = []
    dicts = get_dicts()

    table1 = {}
    table1["field"] = ["gazetteerId", "gazetteerName", "villageId", "villageName", "province", "city", "county",
                       "category1",
                       "data", "unit"]
    table1["data"] = []
    table1["tableNameChinese"] = "村庄基本信息"
    dicts[1] = table1

    # 村志信息
    table2 = {}
    table2["field"] = ["villageId", "villageName", "gazetteerId", "gazetteerName", "publishYear", "publishType"]
    table2["data"] = []
    table2["tableNameChinese"] = "村志基本信息"
    dicts[2] = table2

    table2func = {}
    table2func[3] = getNaturalDisaster
    table2func[4] = getNaturalEnvironment
    table2func[5] = getMilitary
    table2func[6] = getEduaction
    table2func[7] = getEconomy
    table2func[8] = getFamilyPlanning
    table2func[9] = getPopulation
    table2func[10] = getEthnicgroups
    table2func[11] = getFourthlastName
    table2func[12] = getFirstAvailabilityorPurchase

    # For every node in the village and we want to change
    temp = {}
    for village_id in villageid:

        mycursor.execute(
            "SELECT gazetteerTitle_村志书名 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(village_id))
        result = mycursor.fetchone()
        if result == None:
            return jsonify(
                {"code": 4004, "message": "The village id {} is not exsit! Please change.".format(village_id)})

        gazetteerName = result[0]

        for i in getVillage(mycursor, village_id, gazetteerName)["data"]:
            table1["data"].append(i)

        for i in getGazetteer(mycursor, village_id, gazetteerName)["data"]:
            table2["data"].append(i)
        village_year = []
        for index in indexes:
            newTable = dicts[index]  # table3~12
            temp[index] = newTable  # {3: table3...}
            if index == 1 or index == 2:
                continue
            else:
                func = table2func[index]
                res = func(mycursor, village_id, gazetteerName, year, year_range)

                for j in res["data"]:
                    newTable["data"].append(j)  # table3~12["data"].append(i) =>

                print("years are", res["year"])

                if "year" in res.keys():
                    for y in res["year"]:
                        village_year.append(res["year"])
                        newTable["year"].append({village_id: village_year})

    table.append(table1)
    table.append(table2)
    for index in indexes:
        if index == 1 or index == 2:
            continue
        else:
            table.append(temp[index])
    return jsonify(table)


def getVillage(mycursor, village_id, gazetteerName):
    table = {}
    table["data"] = []
    # Get province county villageName city
    mycursor.execute(
        "SELECT p.nameHanyuPinyin_省汉语拼音, ci.nameHanyuPinyin_市汉语拼音 , co.nameHanyuPinyin_县或区汉语拼音, v.nameHanyuPinyin_村名汉语拼音  FROM villageCountyCityProvince_村县市省 vccp JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码={};".format(
            village_id))
    allNames = mycursor.fetchone()
    province = allNames[0]
    city = allNames[1]
    county = allNames[2]
    villageName = allNames[3]

    mycursor.execute(
        "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.villageInnerId_村庄内部代码={} AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码".format(
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

    mycursor.execute("SELECT nameHanyuPinyin_村名汉语拼音 FROM village_村 WHERE gazetteerId_村志代码={}".format(village_id))
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


def getNaturalDisaster(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []
    result_dict["year range"] = "natural disaster doesn't have year range option"
    if year != None:
        mycursor.execute("SELECT year_年份 FROM naturalDisasters_自然灾害 WHERE villageInnerId_村庄内部代码={}".format(village_id))
        all_years = mycursor.fetchall()
        all_years = [i[0] for i in all_years]
        for i in year:
            if i not in all_years:
                result_dict["year_only_empty"].append(i)
                if all_years[closest(all_years, i)] not in year:
                    mycursor.execute(
                        "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
                        " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
                        "AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份={}".format(
                            village_id, all_years[closest(all_years, i)]))
                    disasterList = mycursor.fetchall()
                    result_dict["year_only"].append(all_years[closest(all_years, i)])
                    for item in disasterList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["year"] = item[1]
                        d["category1"] = item[0]
                        table["data"].append(d)

            else:
                mycursor.execute(
                    "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
                    " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
                    "AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份={}".format(
                        village_id, i))
                disasterList = mycursor.fetchall()

                result_dict["year_only"].append(i)
                for item in disasterList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["year"] = item[1]
                    d["category1"] = item[0]
                    table["data"].append(d)

        table["year"].append({"naturaldisaster": result_dict})

    else:
        mycursor.execute(
            "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
            " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
            "AND a.categoryId_类别代码=b.categoryId_类别代码".format(
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


def getNaturalEnvironment(mycursor, village_id, gazetteerName, year=None, year_range=None):
    table = {}
    table["data"] = []
    mycursor.execute(
        "SELECT a.data_数据, b.name_名称, c.name_名称 FROM naturalEnvironment_自然环境 as a, naturalEnvironmentCategory_自然环境类 as b,naturalEnvironmentUnit_自然环境单位 as c \
        WHERE villageInnerId_村庄内部代码={} AND a.categoryId_类别代码=b.categoryId_类别代码 \
        AND a.unitId_单位代码=c.unitId_单位代码".format(
            village_id))
    naturalList = mycursor.fetchall()

    for item in naturalList:
        d = {}
        d["gazetteerName"] = gazetteerName
        d["gazetteerId"] = village_id
        d["data"] = item[0]
        d["category1"] = item[1]
        d["unit"] = item[2]
        table["data"].append(d)
    return table


def closest(same_year, year):
    answer = []
    for i in same_year:
        answer.append(abs(year - i))
    return answer.index(min(answer))


def getMilitary(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"military": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM military_军事 as m WHERE gazetteerId_村志代码={} AND m.startYear_开始年=m.endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute(
                        "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                        m.endYear_结束年, data_数据, mu.name_名称\
                     FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                     JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                     WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id,
                                                                                                   same_years[idx],
                                                                                                   same_years[idx]))
                    militraryList = mycursor.fetchall()
                    for i, item in enumerate(militraryList):
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                            parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                            d["category1"] = parent
                            d["category2"] = item[2]

                        else:
                            d["category1"] = item[2]
                            d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id

                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)

            else:
                result_dict["year_only"].append(i)

                mycursor.execute(
                    "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                    m.endYear_结束年, data_数据, mu.name_名称\
                 FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                 JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                 WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id, i, i))
                militraryList = mycursor.fetchall()
                for i, item in enumerate(militraryList):
                    d = {}
                    if item[1] != None:
                        mycursor.execute(
                            "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                        parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                        d["category1"] = parent
                        d["category2"] = item[2]

                    else:
                        d["category1"] = item[2]
                        d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    elif year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT m.startYear_开始年, m.endYear_结束年 FROM  military_军事 as m WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute(
                            "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                            m.endYear_结束年, data_数据, mu.name_名称\
                         FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                         JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                         WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id,
                                                                                                       start, end))
                        militraryList = mycursor.fetchall()
                        for i, item in enumerate(militraryList):
                            d = {}
                            if item[1] != None:
                                mycursor.execute(
                                    "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                                d["category1"] = parent
                                d["category2"] = item[2]

                            else:
                                d["category1"] = item[2]
                                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)


        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute(
                "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                m.endYear_结束年, data_数据, mu.name_名称\
             FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
             JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
             WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id, start_year,
                                                                                           end_year))
            militraryList = mycursor.fetchall()
            for i, item in enumerate(militraryList):
                d = {}
                if item[1] != None:
                    mycursor.execute(
                        "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                    parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                    d["category1"] = parent
                    d["category2"] = item[2]

                else:
                    d["category1"] = item[2]
                    d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id

                d["startYear"] = item[3]
                d["endYear"] = item[4]

                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)


    # when year_range and year are all None we return the list with all year
    else:
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
                mycursor.execute(
                    "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                d["category1"] = parent
                d["category2"] = item[2]

            else:
                d["category1"] = item[2]
                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"military": result_dict})
    return table


def getEduaction(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"education": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  education_教育 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                        ON e.unitId_单位代码=eu.unitId_单位代码 \
                        WHERE e.gazetteerId_村志代码={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(int(village_id),
                                                                                                        same_years[idx],
                                                                                                        same_years[
                                                                                                                                                 idx]))

                    educationList = mycursor.fetchall()

                    for item in educationList:
                        d = {}
                        if item[1] != None:
                            d["category2"] = "受教育程度 Highest Level of Education"
                        else:
                            d["category2"] = "null"
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category3"] = "null"
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)
            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                        ON e.unitId_单位代码=eu.unitId_单位代码 \
                                        WHERE e.gazetteerId_村志代码={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                    int(village_id), i, i))

                educationList = mycursor.fetchall()

                for item in educationList:
                    d = {}
                    if item[1] != None:
                        d["category2"] = "受教育程度 Highest Level of Education"
                    else:
                        d["category2"] = "null"
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category3"] = "null"
                    d["category1"] = item[2]
                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM education_教育 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                       ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                       ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                       WHERE e.gazetteerId_村志代码={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                            int(village_id), start, end))

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

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                                   ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                                   ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                   WHERE e.gazetteerId_村志代码={} AND  e.startYear_开始年={} AND e.endYear_结束年={}".format(
                int(village_id), start_year, end_year))

            educationList = mycursor.fetchall()

            for item in educationList:
                d = {}
                if item[1] != None:
                    d["category2"] = "受教育程度 Highest Level of Education"
                else:
                    d["category2"] = "null"
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category3"] = "null"
                d["category1"] = item[2]
                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    else:
        mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                ON e.unitId_单位代码=eu.unitId_单位代码 \
                                WHERE e.gazetteerId_村志代码={}".format(int(village_id)))

        educationList = mycursor.fetchall()

        for item in educationList:
            d = {}
            if item[1] != None:
                d["category2"] = "受教育程度 Highest Level of Education"
            else:
                d["category2"] = "null"
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category3"] = "null"
            d["category1"] = item[2]
            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"education": result_dict})

    return table


def getEconomy(mycursor, village_id, gazetteerName, year, year_range):
    transdata = 0
    transunit = 0
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"economy": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM economy_经济 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                          WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(village_id,
                                                                                                           same_years[
                                                                                                               idx],
                                                                                                           same_years[
                                                                                                                                                    idx]))
                    econmialList = mycursor.fetchall()
                    for item in econmialList:
                        d = {}
                        # the category2 also has two upper layer
                        if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                            mycursor.execute(
                                "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                    item[1]))
                            categoryList = mycursor.fetchone()
                            categoryId, d["category2"] = categoryList[0], categoryList[1]

                            mycursor.execute(
                                "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                            d["category3"] = mycursor.fetchone()[0]

                        # the category2 has no upper layer
                        elif item[1] == None:
                            d["category3"] = "null"
                            d["category2"] = "null"

                        # the category2 has onw upper layers
                        else:
                            mycursor.execute(
                                "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                            d["category2"] = mycursor.fetchone()[0]
                            d["category3"] = "null"

                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        if d["category1"] == "总产值 Gross Output Value":
                            if item[6] == "万元 10K yuan":
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == "元 yuan":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 0.0001
                                if item[6] == "百元 100 yuan":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 0.01
                                if item[6] == "千万元 10 millions":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 1000
                                if item[6] == "亿元 100 millions":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 10000
                        if d["category1"] == '集体经济收入 Collective Economic Income':
                            if item[6] == "万元 10K yuan":
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == "元 yuan":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 0.0001
                                if item[6] == "百元 100 yuan":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 0.01
                                if item[6] == "千万元 10 millions":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 1000
                                if item[6] == "亿元 100 millions":
                                    transunit = "万元 10K yuan"
                                    transdata = item[5]
                                    transdata = transdata * 10000
                        if d["category1"] == '耕地面积 Cultivated Area':
                            if item[6] == "亩 mu":
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == '公顷 hectares':
                                    transunit = "亩 mu"
                                    transdata = item[5]
                                    transdata = transdata * 2.470912
                                if item[6] == '平方米 square meters':
                                    transunit = "亩 mu"
                                    transdata = item[5]
                                    transdata = transdata * 0.0002470912
                        if d["category1"] == '粮食总产量 Total Grain Output':
                            if item[6] == '公斤 kilograms':
                                transdata = item[5]
                                transunit = item[6]
                                pass
                            else:
                                if item[6] == '斤 jin':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 0.5
                                if item[6] == '千斤 1K jin':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 1000 * 0.5
                                if item[6] == '万斤 10K jin':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 10000 * 0.5
                                if item[6] == '百公斤 100 kilograms':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 100
                                if item[6] == '万公斤 10K kilograms':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 10000
                                if item[6] == '吨 tons':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 1000
                                if item[6] == '万吨 10K tons':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 10000000
                                if item[6] == '担 dan':
                                    transunit = '公斤 kilograms'
                                    transdata = item[5]
                                    transdata = transdata * 100
                        if d["category1"] == '用电量 Electricity Consumption':
                            if item[6] == '千瓦时/度 kilowatt hours':
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == '千度 1K kilowatt hours':
                                    transunit = '千瓦时/度 kilowatt hours'
                                    transdata = item[5]
                                    transdata = transdata * 1000
                                if item[6] == '万千瓦时/万度 10K kilowatt hours':
                                    transunit = '千瓦时/度 kilowatt hours'
                                    transdata = item[5]
                                    transdata = transdata * 10000
                                if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                                    transunit = '千瓦时/度 kilowatt hours'
                                    transdata = item[5]
                                    transdata = transdata * 1000000
                        if d["category1"] == '电价 Electricity Price':
                            if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                                    transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                                    transdata = item[5]
                                    transdata = transdata * 0.001
                        if d["category1"] == '用水量 Water Consumption':
                            if item[6] == '立方米 cubic meters':
                                transdata = item[5]
                                transunit = item[6]
                                pass
                            else:
                                if item[6] == '万立方米 10K cubic meters':
                                    transunit = '立方米 cubic meters'
                                    transdata = item[5]
                                    transdata = transdata * 10000
                        if d["category1"] == '水价 Water Price':
                            if item[6] == '元每立方米 yuan per cubic meter':
                                transunit = item[6]
                                transdata = item[5]
                                pass
                            else:
                                if item[6] == '元每吨 yuan per ton':
                                    transunit = '元每立方米 yuan per cubic meter'
                                    transdata = item[5]
                                if item[6] == '元每亩 yuan per mu':
                                    transunit = item[6]
                                    transdata = item[5]
                        if d["category1"] == '人均收入 Per Capita Income':
                            if item[6] == '元 yuan':
                                transdata = item[5]
                                transunit = item[6]
                                pass
                            else:
                                if item[6] == '万元 10K yuan':
                                    transunit = '元 yuan'
                                    transdata = item[5]
                                    transdata = transdata * 10000
                        if d["category1"] == '人均居住面积 Per Capita Living Space':
                            if item[6] == '平方米 square meters':
                                transdata = item[5]
                                transunit = item[6]
                                pass
                        # d["data"] = item[5]
                        # d["unit"] = item[6]
                        d["data"] = transdata
                        d["unit"] = transunit

                        table["data"].append(d)
            else:
                result_dict["year_only"].append(i)

                mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                                          WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                    village_id, i, i))
                econmialList = mycursor.fetchall()
                for item in econmialList:
                    d = {}
                    # the category2 also has two upper layer
                    if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                        mycursor.execute(
                            "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                item[1]))
                        categoryList = mycursor.fetchone()
                        categoryId, d["category2"] = categoryList[0], categoryList[1]

                        mycursor.execute(
                            "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                        d["category3"] = mycursor.fetchone()[0]

                    # the category2 has no upper layer
                    elif item[1] == None:
                        d["category3"] = "null"
                        d["category2"] = "null"

                    # the category2 has onw upper layers
                    else:
                        mycursor.execute(
                            "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                        d["category2"] = mycursor.fetchone()[0]
                        d["category3"] = "null"

                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category1"] = item[2]
                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    if d["category1"] == "总产值 Gross Output Value":
                        if item[6] == "万元 10K yuan":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == "元 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.0001
                            if item[6] == "百元 100 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.01
                            if item[6] == "千万元 10 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == "亿元 100 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '集体经济收入 Collective Economic Income':
                        if item[6] == "万元 10K yuan":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == "元 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.0001
                            if item[6] == "百元 100 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.01
                            if item[6] == "千万元 10 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == "亿元 100 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '耕地面积 Cultivated Area':
                        if item[6] == "亩 mu":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '公顷 hectares':
                                transunit = "亩 mu"
                                transdata = item[5]
                                transdata = transdata * 2.470912
                            if item[6] == '平方米 square meters':
                                transunit = "亩 mu"
                                transdata = item[5]
                                transdata = transdata * 0.0002470912
                    if d["category1"] == '粮食总产量 Total Grain Output':
                        if item[6] == '公斤 kilograms':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '斤 jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 0.5
                            if item[6] == '千斤 1K jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 1000 * 0.5
                            if item[6] == '万斤 10K jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000 * 0.5
                            if item[6] == '百公斤 100 kilograms':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 100
                            if item[6] == '万公斤 10K kilograms':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000
                            if item[6] == '吨 tons':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == '万吨 10K tons':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000000
                            if item[6] == '担 dan':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 100
                    if d["category1"] == '用电量 Electricity Consumption':
                        if item[6] == '千瓦时/度 kilowatt hours':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '千度 1K kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == '万千瓦时/万度 10K kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 10000
                            if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 1000000
                    if d["category1"] == '电价 Electricity Price':
                        if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                                transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                                transdata = item[5]
                                transdata = transdata * 0.001
                    if d["category1"] == '用水量 Water Consumption':
                        if item[6] == '立方米 cubic meters':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '万立方米 10K cubic meters':
                                transunit = '立方米 cubic meters'
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '水价 Water Price':
                        if item[6] == '元每立方米 yuan per cubic meter':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '元每吨 yuan per ton':
                                transunit = '元每立方米 yuan per cubic meter'
                                transdata = item[5]
                            if item[6] == '元每亩 yuan per mu':
                                transunit = item[6]
                                transdata = item[5]
                    if d["category1"] == '人均收入 Per Capita Income':
                        if item[6] == '元 yuan':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '万元 10K yuan':
                                transunit = '元 yuan'
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '人均居住面积 Per Capita Living Space':
                        if item[6] == '平方米 square meters':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                    # d["data"] = item[5]
                    # d["unit"] = item[6]
                    d["data"] = transdata
                    d["unit"] = transunit

                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM  economy_经济 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()
        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                  e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                  ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                  ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                  WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                            village_id, start, end))
                        econmialList = mycursor.fetchall()
                        for item in econmialList:
                            d = {}
                            # the category2 also has two upper layer
                            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                                mycursor.execute(
                                    "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                        item[1]))
                                categoryList = mycursor.fetchone()
                                categoryId, d["category2"] = categoryList[0], categoryList[1]

                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                        categoryId))
                                d["category3"] = mycursor.fetchone()[0]

                            # the category2 has no upper layer
                            elif item[1] == None:
                                d["category3"] = "null"
                                d["category2"] = "null"

                            # the category2 has onw upper layers
                            else:
                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                                d["category2"] = mycursor.fetchone()[0]
                                d["category3"] = "null"

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["category1"] = item[2]
                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            if d["category1"] == "总产值 Gross Output Value":
                                if item[6] == "万元 10K yuan":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == "元 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.0001
                                    if item[6] == "百元 100 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.01
                                    if item[6] == "千万元 10 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == "亿元 100 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '集体经济收入 Collective Economic Income':
                                if item[6] == "万元 10K yuan":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == "元 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.0001
                                    if item[6] == "百元 100 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.01
                                    if item[6] == "千万元 10 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == "亿元 100 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '耕地面积 Cultivated Area':
                                if item[6] == "亩 mu":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '公顷 hectares':
                                        transunit = "亩 mu"
                                        transdata = item[5]
                                        transdata = transdata * 2.470912
                                    if item[6] == '平方米 square meters':
                                        transunit = "亩 mu"
                                        transdata = item[5]
                                        transdata = transdata * 0.0002470912
                            if d["category1"] == '粮食总产量 Total Grain Output':
                                if item[6] == '公斤 kilograms':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '斤 jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 0.5
                                    if item[6] == '千斤 1K jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 1000 * 0.5
                                    if item[6] == '万斤 10K jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000 * 0.5
                                    if item[6] == '百公斤 100 kilograms':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 100
                                    if item[6] == '万公斤 10K kilograms':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                                    if item[6] == '吨 tons':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == '万吨 10K tons':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000000
                                    if item[6] == '担 dan':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 100
                            if d["category1"] == '用电量 Electricity Consumption':
                                if item[6] == '千瓦时/度 kilowatt hours':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '千度 1K kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == '万千瓦时/万度 10K kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                                    if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 1000000
                            if d["category1"] == '电价 Electricity Price':
                                if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                                        transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                                        transdata = item[5]
                                        transdata = transdata * 0.001
                            if d["category1"] == '用水量 Water Consumption':
                                if item[6] == '立方米 cubic meters':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '万立方米 10K cubic meters':
                                        transunit = '立方米 cubic meters'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '水价 Water Price':
                                if item[6] == '元每立方米 yuan per cubic meter':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '元每吨 yuan per ton':
                                        transunit = '元每立方米 yuan per cubic meter'
                                        transdata = item[5]
                                    if item[6] == '元每亩 yuan per mu':
                                        transunit = item[6]
                                        transdata = item[5]
                            if d["category1"] == '人均收入 Per Capita Income':
                                if item[6] == '元 yuan':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '万元 10K yuan':
                                        transunit = '元 yuan'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '人均居住面积 Per Capita Living Space':
                                if item[6] == '平方米 square meters':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                            # d["data"] = item[5]
                            # d["unit"] = item[6]
                            d["data"] = transdata
                            d["unit"] = transunit

                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                              e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                              ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                              ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                              WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                village_id, start_year, end_year))
            econmialList = mycursor.fetchall()
            for item in econmialList:
                d = {}
                # the category2 also has two upper layer
                if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                    mycursor.execute(
                        "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                            item[1]))
                    categoryList = mycursor.fetchone()
                    categoryId, d["category2"] = categoryList[0], categoryList[1]

                    mycursor.execute(
                        "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                            categoryId))
                    d["category3"] = mycursor.fetchone()[0]

                # the category2 has no upper layer
                elif item[1] == None:
                    d["category3"] = "null"
                    d["category2"] = "null"

                # the category2 has onw upper layers
                else:
                    mycursor.execute(
                        "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                    d["category2"] = mycursor.fetchone()[0]
                    d["category3"] = "null"

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category1"] = item[2]
                d["startYear"] = item[3]
                d["endYear"] = item[4]
                if d["category1"] == "总产值 Gross Output Value":
                    if item[6] == "万元 10K yuan":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == "元 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.0001
                        if item[6] == "百元 100 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.01
                        if item[6] == "千万元 10 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == "亿元 100 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '集体经济收入 Collective Economic Income':
                    if item[6] == "万元 10K yuan":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == "元 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.0001
                        if item[6] == "百元 100 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.01
                        if item[6] == "千万元 10 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == "亿元 100 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '耕地面积 Cultivated Area':
                    if item[6] == "亩 mu":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '公顷 hectares':
                            transunit = "亩 mu"
                            transdata = item[5]
                            transdata = transdata * 2.470912
                        if item[6] == '平方米 square meters':
                            transunit = "亩 mu"
                            transdata = item[5]
                            transdata = transdata * 0.0002470912
                if d["category1"] == '粮食总产量 Total Grain Output':
                    if item[6] == '公斤 kilograms':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '斤 jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 0.5
                        if item[6] == '千斤 1K jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 1000 * 0.5
                        if item[6] == '万斤 10K jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000 * 0.5
                        if item[6] == '百公斤 100 kilograms':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 100
                        if item[6] == '万公斤 10K kilograms':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000
                        if item[6] == '吨 tons':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == '万吨 10K tons':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000000
                        if item[6] == '担 dan':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 100
                if d["category1"] == '用电量 Electricity Consumption':
                    if item[6] == '千瓦时/度 kilowatt hours':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '千度 1K kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == '万千瓦时/万度 10K kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 10000
                        if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 1000000
                if d["category1"] == '电价 Electricity Price':
                    if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                            transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                            transdata = item[5]
                            transdata = transdata * 0.001
                if d["category1"] == '用水量 Water Consumption':
                    if item[6] == '立方米 cubic meters':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '万立方米 10K cubic meters':
                            transunit = '立方米 cubic meters'
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '水价 Water Price':
                    if item[6] == '元每立方米 yuan per cubic meter':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '元每吨 yuan per ton':
                            transunit = '元每立方米 yuan per cubic meter'
                            transdata = item[5]
                        if item[6] == '元每亩 yuan per mu':
                            transunit = item[6]
                            transdata = item[5]
                if d["category1"] == '人均收入 Per Capita Income':
                    if item[6] == '元 yuan':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '万元 10K yuan':
                            transunit = '元 yuan'
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '人均居住面积 Per Capita Living Space':
                    if item[6] == '平方米 square meters':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                # d["data"] = item[5]
                # d["unit"] = item[6]
                d["data"] = transdata
                d["unit"] = transunit

                table["data"].append(d)

    else:
        mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
          ON e.unitId_单位代码=eu.unitId_单位代码 \
          WHERE e.gazetteerId_村志代码 ={}".format(village_id))
        econmialList = mycursor.fetchall()
        for item in econmialList:
            d = {}
            # the category2 also has two upper layer
            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                mycursor.execute(
                    "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                categoryList = mycursor.fetchone()
                categoryId, d["category2"] = categoryList[0], categoryList[1]

                mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                d["category3"] = mycursor.fetchone()[0]

            # the category2 has no upper layer
            elif item[1] == None:
                d["category3"] = "null"
                d["category2"] = "null"

            # the category2 has onw upper layers
            else:
                mycursor.execute("SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                d["category2"] = mycursor.fetchone()[0]
                d["category3"] = "null"

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category1"] = item[2]
            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])
            if d["category1"] == "总产值 Gross Output Value":
                if item[6] == "万元 10K yuan":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == "元 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.0001
                    if item[6] == "百元 100 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.01
                    if item[6] == "千万元 10 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == "亿元 100 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '集体经济收入 Collective Economic Income':
                if item[6] == "万元 10K yuan":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == "元 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.0001
                    if item[6] == "百元 100 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.01
                    if item[6] == "千万元 10 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == "亿元 100 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '耕地面积 Cultivated Area':
                if item[6] == "亩 mu":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '公顷 hectares':
                        transunit = "亩 mu"
                        transdata = item[5]
                        transdata = transdata * 2.470912
                    if item[6] == '平方米 square meters':
                        transunit = "亩 mu"
                        transdata = item[5]
                        transdata = transdata * 0.0002470912
            if d["category1"] == '粮食总产量 Total Grain Output':
                if item[6] == '公斤 kilograms':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '斤 jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 0.5
                    if item[6] == '千斤 1K jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 1000 * 0.5
                    if item[6] == '万斤 10K jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000 * 0.5
                    if item[6] == '百公斤 100 kilograms':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 100
                    if item[6] == '万公斤 10K kilograms':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000
                    if item[6] == '吨 tons':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == '万吨 10K tons':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000000
                    if item[6] == '担 dan':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 100
            if d["category1"] == '用电量 Electricity Consumption':
                if item[6] == '千瓦时/度 kilowatt hours':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '千度 1K kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == '万千瓦时/万度 10K kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 10000
                    if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 1000000
            if d["category1"] == '电价 Electricity Price':
                if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                        transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                        transdata = item[5]
                        transdata = transdata * 0.001
            if d["category1"] == '用水量 Water Consumption':
                if item[6] == '立方米 cubic meters':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '万立方米 10K cubic meters':
                        transunit = '立方米 cubic meters'
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '水价 Water Price':
                if item[6] == '元每立方米 yuan per cubic meter':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '元每吨 yuan per ton':
                        transunit = '元每立方米 yuan per cubic meter'
                        transdata = item[5]
                    if item[6] == '元每亩 yuan per mu':
                        transunit = item[6]
                        transdata = item[5]
            if d["category1"] == '人均收入 Per Capita Income':
                if item[6] == '元 yuan':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '万元 10K yuan':
                        transunit = '元 yuan'
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '人均居住面积 Per Capita Living Space':
                if item[6] == '平方米 square meters':
                    transdata = item[5]
                    transunit = item[6]
                    pass
            # d["data"] = item[5]
            # d["unit"] = item[6]
            d["data"] = transdata
            d["unit"] = transunit

            table["data"].append(d)

    table["year"].append({"economy": result_dict})
    return table


def getFamilyPlanning(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"familyplanning": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  familyplanning_计划生育 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                          ON f.unitId_单位代码=fu.unitId_单位代码 \
                          WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(village_id,
                                                                                                           same_years[
                                                                                                               idx],
                                                                                                           same_years[
                                                                                                                                                    idx]))
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

            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                          ON f.unitId_单位代码=fu.unitId_单位代码 \
                                          WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                    village_id, i, i))
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
    if year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM familyplanning_计划生育 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                                                  f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                                                  FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                                                  ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                                                  ON f.unitId_单位代码=fu.unitId_单位代码 \
                                                                  WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                            village_id, start, end))
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
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                                                              f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                                                              FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                                                              ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                                                              ON f.unitId_单位代码=fu.unitId_单位代码 \
                                                                              WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                village_id, start_year, end_year))
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
    else:
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
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)

    table["year"].append({"familyplanning": result_dict})
    return table


def getPopulation(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"population": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM population_人口 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                        WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(village_id,
                                                                                                         same_years[
                                                                                                             idx],
                                                                                                         same_years[
                                                                                                                                                  idx]))
                    populationList = mycursor.fetchall()
                    for item in populationList:
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
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
                else:
                    result_dict["year_only"].append(i)
                    mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                            p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                            FROM population_人口 p JOIN populationcategory_人口类 pc \
                                            ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                            ON p.unitId_单位代码=pu.unitId_单位代码 \
                                            WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                        village_id, i, i))
                    populationList = mycursor.fetchall()
                    for item in populationList:
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
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
    elif year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM population_人口 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()
        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                                    p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                                    FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                                    ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                                    ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                                    WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                            village_id, start, end))
                        populationList = mycursor.fetchall()
                        for item in populationList:
                            d = {}
                            if item[1] != None:
                                mycursor.execute(
                                    "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(
                                        item[1]))
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
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                        WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                village_id, start_year, end_year))
            populationList = mycursor.fetchall()
            for item in populationList:
                d = {}
                if item[1] != None:
                    mycursor.execute(
                        "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
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

    else:
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
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"population": result_dict})
    return table


def getEthnicgroups(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"ethnicgroups": result_dict})
            return table

    if year != None and year != []:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  ethnicGroups_民族 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                          eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                          FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                          ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                          ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                          WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                        village_id, same_years[idx], same_years[idx]))
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
            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                                         eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                         FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                         ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                         ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                         WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                    village_id, i, i))
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

    elif year_range != None and len(year_range) == 2:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM ethnicGroups_民族 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                                                                 eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                 FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                 ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                 ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                 WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                            village_id, start, end))
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
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年, eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                             FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                             ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                             ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                             WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                village_id, start_year, end_year))
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
    else:

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
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)

    table["year"].append({"ethnicgroups": result_dict})
    return table


def getFourthlastName(mycursor, village_id, gazetteerName, year, year_range):
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
                "SELECT nameHanyuPinyin_姓氏汉语拼音 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 ={}".format(
                    nameList[0][z]))
            l.append(mycursor.fetchone()[0])

    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["firstLastNameId"] = l[0]
    d["secondLastNameId"] = l[1]
    d["thirdLastNameId"] = l[2]
    d["fourthLastNameId"] = l[3]
    d["fifthlastNamesId"] = l[4]
    d["totalNumberOfLastNameInVillage"] = nameList[0][-1]

    table["data"].append(d)

    return table


def getFirstAvailabilityorPurchase(mycursor, village_id, gazetteerName, year, year_range):
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
