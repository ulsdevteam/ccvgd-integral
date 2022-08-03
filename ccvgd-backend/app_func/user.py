import json
import os
import re

import mysql.connector
from flask import Blueprint, jsonify, request, session, send_from_directory
from flask_cors import CORS
from status_code import *
from config import *

user_blueprint = Blueprint('user', __name__)
CORS(user_blueprint)

@user_blueprint.route("/login", methods=["POST"], strict_slashes=False)
def user_login():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data.get("username")
    password = json_data.get("password")

    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_username,
        password=mysql_password,
        port=mysql_port,
        database=mysql_database)


    mycursor = mydb.cursor()

    mycursor.execute("SELECT * FROM user WHERE username='{}' AND password='{}'".format(username, password))
    if mycursor.fetchone():
        return jsonify({"code":200,"message":"User login successfully!"})
    else:
        return jsonify({"code":409,"message":"No this username is wrong or password is wrong! Please try again"})


@user_blueprint.route("/register", methods=["POST"], strict_slashes=False)
def user_register():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data.get("username")
    password = json_data.get("password")
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

    mycursor.execute("SELECT * FROM user WHERE username='{}'".format(username))
    if mycursor.fetchone():
        return jsonify({"code":407, "message":"This username already used"})
    else:
        try:
            print(mycursor.execute("INSERT INTO user (username, password) VALUES ('{}', '{}') ".format(username, password)))
            mydb.commit()
        except Exception as e:
            print(e)
            return jsonify({"code":406,"message":"Insert username and password fail, please try again!"})
        return jsonify({"code":200,"message":"Register successful!"})