import csv
import json

import mysql.connector
import pandas as pd
import pymysql
from flask import Blueprint, jsonify, request, send_from_directory, send_file
from flask_cors import CORS

from config import *
from .utils import get_dicts

utils_blueprint = Blueprint('utils', __name__)
CORS(utils_blueprint)


@utils_blueprint.route("/province", methods=["GET"], strict_slashes=False)
def get_provience():
    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)
    mycursor = conn.cursor()


    mycursor.execute("SELECT nameChineseCharacters_省汉字 FROM province_省;")
    result = mycursor.fetchall()
    res = []
    for i in result:
        res.append(i[0])
    return jsonify(res)




@utils_blueprint.route("/city", methods=["GET"], strict_slashes=False)
def get_city():
    province = request.args.get("province")
    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)


    mycursor = conn.cursor()

    sql = "SELECT city.nameChineseCharacters_市汉字 \
     FROM city_市 city  JOIN villageCountyCityProvince_村县市省 jointable ON \
     city.cityId_市代码=jointable.cityId_市代码 JOIN province_省 province \
      ON province.provinceId_省代码=jointable.provinceId_省代码 WHERE \
      province.nameChineseCharacters_省汉字= %s"

    mycursor.execute(sql,province)
    result = mycursor.fetchall()
    res =set()
    for i in result:
        res.add(i[0])
    res = list(res)
    return jsonify(res)

@utils_blueprint.route("/county", methods=["GET"], strict_slashes=False)
def get_county():
    province = request.args.get("province")
    city = request.args.get("city")
    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)

    mycursor = conn.cursor()

    sql = "SELECT county.nameChineseCharacters_县或区汉字 \
          FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON \
     county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 JOIN \
      city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province \
      ON province.provinceId_省代码=jointable.provinceId_省代码 WHERE \
          province.nameChineseCharacters_省汉字= %s AND city.nameChineseCharacters_市汉字= %s "

    mycursor.execute(sql, (province, city))

    result = mycursor.fetchall()
    res = set()
    for i in result:
        res.add(i[0])
    res = list(res)
    return jsonify(res)


@utils_blueprint.route("/getall", methods=["POST","GET"], strict_slashes=False)
def get_all():

    # get all 1500 village in format
    # {"village_id":,
    #   "village_name":"",
    #   "province":"",
    #   "city":"",
    #   "county":""
    # }

    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)

    mycursor = conn.cursor()

    if request.method == "GET":
        table = []
        sql = "SELECT v.gazetteerId_村志代码, p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字,\
     v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp \
     JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON\
      vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 \
      JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 ;"
        mycursor.execute(sql)
        allNames = mycursor.fetchall()
        if allNames is None or len(allNames) == 0:
            return table

        for item in allNames:
            temp_list = {}
            temp_list["id"] = item[0]
            temp_list["province"] = item[1]
            temp_list["city"] = item[2]
            temp_list["county"] = item[3]
            temp_list["name"] = item[4]
            table.append(temp_list)

        return jsonify(table)

    else:
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        village_name = json_data.get("villageName", None) if "villageName" in json_data.keys() and json_data[
            "villageName"] != "" else None
        province = json_data.get("province", None) if "province" in json_data.keys() and json_data[
            "province"] != "" else None
        city = json_data.get("city", None) if "city" in json_data.keys() and json_data["city"] != "" else None
        county = json_data.get("county", None) if "county" in json_data.keys() and json_data["county"] != "" else None

        map_dict = {
            "v.nameChineseCharacters_村名汉字":village_name,
            "p.nameChineseCharacters_省汉字":province,
            "ci.nameChineseCharacters_市汉字":city,
            "co.nameChineseCharacters_县或区汉字":county
        }
        map_sql = ""
        item_flag = False

        # when there is no other identifiers
        if province is None and city is None and county is None and village_name is not None:
            village_sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
                                         FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
                                        JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN  \
                                  province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN  village_村 \
                                   village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码  \
                                  WHERE village.nameChineseCharacters_村名汉字 LIKE '%{}%';".format(village_name)

            mycursor.execute(village_sql)
            allNames = mycursor.fetchall()
            table = []
            if allNames is None or len(allNames) == 0:
                return jsonify(table)

            for item in allNames:
                temp_list = {}
                temp_list["id"] = item[4]
                temp_list["province"] = item[3]
                temp_list["city"] = item[2]
                temp_list["county"] = item[1]
                temp_list["name"] = item[0]
                table.append(temp_list)
            return jsonify(table)

        for key, value in map_dict.items():
            if value is not None and key != "v.nameChineseCharacters_村名汉字":
                if item_flag == True:
                    map_sql += " " "AND" + " " + key + "=" + "\'" + value + "\'"
                else:
                    item_flag = True
                    map_sql += key + "=" + "\'" + value + "\'"
            if value is not None and key == "v.nameChineseCharacters_村名汉字" and item_flag == True:
                map_sql += "AND" + " " + key + "like" + "\'%" + value + "\'%"
            elif value is not None and key == "v.nameChineseCharacters_村名汉字" and item_flag == False:
                map_sql += key + " " + "LIKE " + "\'%" + value + "%\'"
                item_flag = True
                
        print(map_sql)
        if map_sql == "":
            return jsonify({"code":4011,"message":"At least has one name pass in!"})
        table = []
        sql = "SELECT v.gazetteerId_村志代码, p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字,\
             v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp \
             JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON\
              vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 \
              JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE {}".format(map_sql)
        mycursor.execute(sql)
        allNames = mycursor.fetchall()
        if allNames is None or len(allNames) == 0:
            return jsonify(table)

        for item in allNames:
            temp_list = {}
            temp_list["id"] = item[0]
            temp_list["province"] = item[1]
            temp_list["city"] = item[2]
            temp_list["county"] = item[3]
            temp_list["name"] = item[4]
            table.append(temp_list)

        return jsonify(table)
