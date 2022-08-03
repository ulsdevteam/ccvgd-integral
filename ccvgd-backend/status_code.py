class MESSAGE:
  OK = "200"
  DBERR = "4001"
  NODATA = "4002"
  DATAEXIST = "4003"
  DATAERR = "4004"
  SESSIONERR = "4101"
  LOGINERR = "4102"
  PARAMERR = "4103"
  USERERR = "4104"
  ROLEERR = "4105"
  PWDERR = "4106"
  REQERR = "4201"
  IPERR = "4202"
  THIRDERR = "4301"
  IOERR = "4302"
  SERVERERR = "4500"
  UNKOWNERR = "4501"

ret_ch_map = {
MESSAGE.OK: u"成功",
  MESSAGE.DBERR: u"数据库错误，请稍后再试",
  MESSAGE.NODATA: u"没有该数据，请重新传入",
  MESSAGE.DATAEXIST: u"该数据已存在，请重新传入",
  MESSAGE.DATAERR: u"数据格式错误",
  # MESSAGE.SESSIONERR: u"User not logged in",
  # MESSAGE.LOGINERR: u"User login failed",
  MESSAGE.PARAMERR: u"参数错误，请重新传入",
  # MESSAGE.USERERR: u"The user does not exist or is not activated",
  # MESSAGE.ROLEERR: u"User identity error",
  # MESSAGE.PWDERR: u"Wrong password",
  MESSAGE.REQERR: u"非法请求，或者请求数量有限",
  MESSAGE.IPERR: u"IP 地址重定向",
  # MESSAGE.THIRDERR: u"Third party system error",
  MESSAGE.IOERR: u"数据读出错误",
  MESSAGE.SERVERERR: u"网络问题",
  # MESSAGE.UNKOWNERR: u"Unknown error",
}
ret_en_map = {
  MESSAGE.OK: u"Success",
  MESSAGE.DBERR: u"Database Error，please wait",
  MESSAGE.NODATA: u"No data, please input again",
  MESSAGE.DATAEXIST: u"This data already exist, please input again",
  MESSAGE.DATAERR: u"Data format error,please try again",
  # MESSAGE.SESSIONERR: u"User not logged in",
  # MESSAGE.LOGINERR: u"User login failed",
  MESSAGE.PARAMERR: u"Parameter error, please try again",
  # MESSAGE.USERERR: u"The user does not exist or is not activated",
  # MESSAGE.ROLEERR: u"User identity error",
  # MESSAGE.PWDERR: u"Wrong password",
  MESSAGE.REQERR: u"Illegal request or limited number of requests",
  MESSAGE.IPERR: u"IP restricted",
  # MESSAGE.THIRDERR: u"Third party system error",
  MESSAGE.IOERR: u"File read / write error",
  MESSAGE.SERVERERR: u"Internal error",
  # MESSAGE.UNKOWNERR: u"Unknown error",
}
