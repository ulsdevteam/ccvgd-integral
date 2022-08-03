# CCVG api doc

## muilti-village search api

### advance search POST

http://ngrok.luozm.me:8395/ccvg/advancesearch/


input value example
#### Without year
```json
{
    "villageid":["1","2"],
    "topic": ["population"]
}
```

return 


return item explaintion

| KEY      | VALUE |
| ----------- | ----------- |
| data      | data return        |
| field      | item's name used at data part       |
| tableNameChinese      | 烟台市       |

return year details

| KEY      | VALUE |
| ----------- | ----------- |
| village id      | number       |
| topic name     | eg. population       |
| year_only     | single year matched with input      |
| year_only_empty     | single year don't have data     |
| year_range     | a list contain [start year, end year]  |
| year_range_empty     | years don't have exactly match result   |




```json
[
    {
        "data": [
            {
                "gazetteerId": "411082100218",
                "gazetteerName": "太平店村志",
                "publishType": "正式出版 Formal",
                "publishYear": "2009",
                "villageId": "1",
                "villageName": "太平店村"
            }
        ],
        "field": [
            "village_id",
            "villageName",
            "gazetteerId",
            "gazetteerName",
            "publishYear",
            "publishType"
        ],
        "tableNameChinese": "村志基本信息"
    },
    {
        "data": [
            {
                "category1": "人口 Population",
                "category2": "总人口 Total Population",
                "data": 3218.0,
                "endYear": 2001,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2001,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "人口 Population",
                "category2": "总人口 Total Population",
                "data": 3287.0,
                "endYear": 2008,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2008,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "户数 Number of Households",
                "category2": "null",
                "data": 802.0,
                "endYear": 2008,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2008,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "迁入 Migration In",
                "category2": "知识青年 Educated Youth",
                "data": 36.0,
                "endYear": 1965,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 1965,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            }
        ],
        "field": [
            "villageId",
            "gazetteerName",
            "gazetteerId",
            "category1",
            "category2",
            "startYear",
            "endYear",
            "data",
            "unit"
        ],
        "tableNameChinese": "人口",
        "year": [
            {
                "1": [
                    {
                        "population": {
                            "year_only": [
                                2001,
                                2008,
                                1965
                            ],
                            "year_only_empty": [],
                            "year_range": [
                                [
                                    2008,
                                    2008
                                ]
                            ],
                            "year_range_empty": []
                        }
                    }
                ]
            }
        ]
    }
]
```

#### Only use year

**Here single year of 2001 and 2008 which are known have value**

```json
{
    "villageid":["1","2"],
    "topic": ["population"],
    "year":[2001,2008]
}
```

return 



```json
[
    {
        "data": [
            {
                "gazetteerId": "411082100218",
                "gazetteerName": "太平店村志",
                "publishType": "正式出版 Formal",
                "publishYear": "2009",
                "villageId": "1",
                "villageName": "太平店村"
            }
        ],
        "field": [
            "village_id",
            "villageName",
            "gazetteerId",
            "gazetteerName",
            "publishYear",
            "publishType"
        ],
        "tableNameChinese": "村志基本信息"
    },
    {
        "data": [
            {
                "category1": "人口 Population",
                "category2": "总人口 Total Population",
                "data": 3218.0,
                "endYear": 2001,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2001,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "人口 Population",
                "category2": "总人口 Total Population",
                "data": 3287.0,
                "endYear": 2008,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2008,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "户数 Number of Households",
                "category2": "null",
                "data": 802.0,
                "endYear": 2008,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 2008,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            }
        ],
        "field": [
            "villageId",
            "gazetteerName",
            "gazetteerId",
            "category1",
            "category2",
            "startYear",
            "endYear",
            "data",
            "unit"
        ],
        "tableNameChinese": "人口",
        "year": [
            {
                "1": [
                    {
                        "population": {
                            "year_only": [
                                2001,
                                2008
                            ],
                            "year_only_empty": [],
                            "year_range": [],
                            "year_range_empty": []
                        }
                    }
                ]
            }
        ]
    }
]
```

If the single year don't have any value then will return none value

input 
```json
{
    "villageid":["1"],
    "topic": ["population"],
    "year":[2009]
}
```

return 

```json
[
    {
        "data": [
            {
                "gazetteerId": "411082100218",
                "gazetteerName": "太平店村志",
                "publishType": "正式出版 Formal",
                "publishYear": "2009",
                "villageId": "1",
                "villageName": "太平店村"
            }
        ],
        "field": [
            "village_id",
            "villageName",
            "gazetteerId",
            "gazetteerName",
            "publishYear",
            "publishType"
        ],
        "tableNameChinese": "村志基本信息"
    },
    {
        "data": [],
        "field": [
            "villageId",
            "gazetteerName",
            "gazetteerId",
            "category1",
            "category2",
            "startYear",
            "endYear",
            "data",
            "unit"
        ],
        "tableNameChinese": "人口",
        "year": [
            {
                "1": [
                    {
                        "population": {
                            "year_only": [
                                2009
                            ],
                            "year_only_empty": [],
                            "year_range": [],
                            "year_range_empty": []
                        }
                    }
                ]
            }
        ]
    }
]
```


#### Only use year_range

**Here single year of 2001 and 2008 which are known have value**

input ***The year item should also added***

```json
{
    "villageid":["1"],
    "topic": ["population"],
    "year":[],
    "year_range":[1949, 2000]
}
```

return 

```json
[
    {
        "data": [
            {
                "gazetteerId": "411082100218",
                "gazetteerName": "太平店村志",
                "publishType": "正式出版 Formal",
                "publishYear": "2009",
                "villageId": "1",
                "villageName": "太平店村"
            }
        ],
        "field": [
            "village_id",
            "villageName",
            "gazetteerId",
            "gazetteerName",
            "publishYear",
            "publishType"
        ],
        "tableNameChinese": "村志基本信息"
    },
    {
        "data": [
            {
                "category1": "迁入 Migration In",
                "category2": "知识青年 Educated Youth",
                "data": 36.0,
                "endYear": 1965,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 1965,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "迁入 Migration In",
                "category2": "知识青年 Educated Youth",
                "data": 36.0,
                "endYear": 1965,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 1965,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            },
            {
                "category1": "迁入 Migration In",
                "category2": "知识青年 Educated Youth",
                "data": 36.0,
                "endYear": 1965,
                "gazetteerId": "1",
                "gazetteerName": "太平店村志",
                "startYear": 1965,
                "unit": "人数 number of people",
                "villageId": "411082100218"
            }
        ],
        "field": [
            "villageId",
            "gazetteerName",
            "gazetteerId",
            "category1",
            "category2",
            "startYear",
            "endYear",
            "data",
            "unit"
        ],
        "tableNameChinese": "人口",
        "year": [
            {
                "1": [
                    {
                        "population": {
                            "year_only": [],
                            "year_only_empty": [],
                            "year_range": [
                                [
                                    1949,
                                    1949
                                ],
                                [
                                    1950,
                                    1950
                                ],
                                [
                                    1951,
                                    1951
                                ],
                                [
                                    1952,
                                    1952
                                ],
                                [
                                    1953,
                                    1953
                                ],
                                [
                                    1954,
                                    1954
                                ],
                                [
                                    1955,
                                    1955
                                ],
                                [
                                    1956,
                                    1956
                                ],
                                [
                                    1957,
                                    1957
                                ],
                                [
                                    1958,
                                    1958
                                ],
                                [
                                    1959,
                                    1959
                                ],
                                [
                                    1960,
                                    1960
                                ],
                                [
                                    1961,
                                    1961
                                ],
                                [
                                    1962,
                                    1962
                                ],
                                [
                                    1963,
                                    1963
                                ],
                                [
                                    1964,
                                    1964
                                ],
                                [
                                    1965,
                                    1965
                                ],
                                [
                                    1966,
                                    1966
                                ],
                                [
                                    1967,
                                    1967
                                ],
                                [
                                    1968,
                                    1968
                                ],
                                [
                                    1969,
                                    1969
                                ],
                                [
                                    1970,
                                    1970
                                ],
                                [
                                    1971,
                                    1971
                                ],
                                [
                                    1972,
                                    1972
                                ],
                                [
                                    1973,
                                    1973
                                ],
                                [
                                    1974,
                                    1974
                                ],
                                [
                                    1975,
                                    1975
                                ],
                                [
                                    1976,
                                    1976
                                ],
                                [
                                    1977,
                                    1977
                                ],
                                [
                                    1978,
                                    1978
                                ],
                                [
                                    1979,
                                    1979
                                ],
                                [
                                    1980,
                                    1980
                                ],
                                [
                                    1981,
                                    1981
                                ],
                                [
                                    1982,
                                    1982
                                ],
                                [
                                    1983,
                                    1983
                                ],
                                [
                                    1984,
                                    1984
                                ],
                                [
                                    1985,
                                    1985
                                ],
                                [
                                    1986,
                                    1986
                                ],
                                [
                                    1987,
                                    1987
                                ],
                                [
                                    1988,
                                    1988
                                ],
                                [
                                    1989,
                                    1989
                                ],
                                [
                                    1990,
                                    1990
                                ],
                                [
                                    1991,
                                    1991
                                ],
                                [
                                    1992,
                                    1992
                                ],
                                [
                                    1993,
                                    1993
                                ],
                                [
                                    1994,
                                    1994
                                ],
                                [
                                    1995,
                                    1995
                                ],
                                [
                                    1996,
                                    1996
                                ],
                                [
                                    1997,
                                    1997
                                ],
                                [
                                    1998,
                                    1998
                                ],
                                [
                                    1999,
                                    1999
                                ],
                                [
                                    2000,
                                    2000
                                ],
                                [
                                    1949,
                                    1949
                                ],
                                [
                                    1950,
                                    1950
                                ],
                                [
                                    1951,
                                    1951
                                ],
                                [
                                    1952,
                                    1952
                                ],
                                [
                                    1953,
                                    1953
                                ],
                                [
                                    1954,
                                    1954
                                ],
                                [
                                    1955,
                                    1955
                                ],
                                [
                                    1956,
                                    1956
                                ],
                                [
                                    1957,
                                    1957
                                ],
                                [
                                    1958,
                                    1958
                                ],
                                [
                                    1959,
                                    1959
                                ],
                                [
                                    1960,
                                    1960
                                ],
                                [
                                    1961,
                                    1961
                                ],
                                [
                                    1962,
                                    1962
                                ],
                                [
                                    1963,
                                    1963
                                ],
                                [
                                    1964,
                                    1964
                                ],
                                [
                                    1965,
                                    1965
                                ],
                                [
                                    1966,
                                    1966
                                ],
                                [
                                    1967,
                                    1967
                                ],
                                [
                                    1968,
                                    1968
                                ],
                                [
                                    1969,
                                    1969
                                ],
                                [
                                    1970,
                                    1970
                                ],
                                [
                                    1971,
                                    1971
                                ],
                                [
                                    1972,
                                    1972
                                ],
                                [
                                    1973,
                                    1973
                                ],
                                [
                                    1974,
                                    1974
                                ],
                                [
                                    1975,
                                    1975
                                ],
                                [
                                    1976,
                                    1976
                                ],
                                [
                                    1977,
                                    1977
                                ],
                                [
                                    1978,
                                    1978
                                ],
                                [
                                    1979,
                                    1979
                                ],
                                [
                                    1980,
                                    1980
                                ],
                                [
                                    1981,
                                    1981
                                ],
                                [
                                    1982,
                                    1982
                                ],
                                [
                                    1983,
                                    1983
                                ],
                                [
                                    1984,
                                    1984
                                ],
                                [
                                    1985,
                                    1985
                                ],
                                [
                                    1986,
                                    1986
                                ],
                                [
                                    1987,
                                    1987
                                ],
                                [
                                    1988,
                                    1988
                                ],
                                [
                                    1989,
                                    1989
                                ],
                                [
                                    1990,
                                    1990
                                ],
                                [
                                    1991,
                                    1991
                                ],
                                [
                                    1992,
                                    1992
                                ],
                                [
                                    1993,
                                    1993
                                ],
                                [
                                    1994,
                                    1994
                                ],
                                [
                                    1995,
                                    1995
                                ],
                                [
                                    1996,
                                    1996
                                ],
                                [
                                    1997,
                                    1997
                                ],
                                [
                                    1998,
                                    1998
                                ],
                                [
                                    1999,
                                    1999
                                ],
                                [
                                    2000,
                                    2000
                                ],
                                [
                                    1949,
                                    1949
                                ],
                                [
                                    1950,
                                    1950
                                ],
                                [
                                    1951,
                                    1951
                                ],
                                [
                                    1952,
                                    1952
                                ],
                                [
                                    1953,
                                    1953
                                ],
                                [
                                    1954,
                                    1954
                                ],
                                [
                                    1955,
                                    1955
                                ],
                                [
                                    1956,
                                    1956
                                ],
                                [
                                    1957,
                                    1957
                                ],
                                [
                                    1958,
                                    1958
                                ],
                                [
                                    1959,
                                    1959
                                ],
                                [
                                    1960,
                                    1960
                                ],
                                [
                                    1961,
                                    1961
                                ],
                                [
                                    1962,
                                    1962
                                ],
                                [
                                    1963,
                                    1963
                                ],
                                [
                                    1964,
                                    1964
                                ],
                                [
                                    1965,
                                    1965
                                ],
                                [
                                    1966,
                                    1966
                                ],
                                [
                                    1967,
                                    1967
                                ],
                                [
                                    1968,
                                    1968
                                ],
                                [
                                    1969,
                                    1969
                                ],
                                [
                                    1970,
                                    1970
                                ],
                                [
                                    1971,
                                    1971
                                ],
                                [
                                    1972,
                                    1972
                                ],
                                [
                                    1973,
                                    1973
                                ],
                                [
                                    1974,
                                    1974
                                ],
                                [
                                    1975,
                                    1975
                                ],
                                [
                                    1976,
                                    1976
                                ],
                                [
                                    1977,
                                    1977
                                ],
                                [
                                    1978,
                                    1978
                                ],
                                [
                                    1979,
                                    1979
                                ],
                                [
                                    1980,
                                    1980
                                ],
                                [
                                    1981,
                                    1981
                                ],
                                [
                                    1982,
                                    1982
                                ],
                                [
                                    1983,
                                    1983
                                ],
                                [
                                    1984,
                                    1984
                                ],
                                [
                                    1985,
                                    1985
                                ],
                                [
                                    1986,
                                    1986
                                ],
                                [
                                    1987,
                                    1987
                                ],
                                [
                                    1988,
                                    1988
                                ],
                                [
                                    1989,
                                    1989
                                ],
                                [
                                    1990,
                                    1990
                                ],
                                [
                                    1991,
                                    1991
                                ],
                                [
                                    1992,
                                    1992
                                ],
                                [
                                    1993,
                                    1993
                                ],
                                [
                                    1994,
                                    1994
                                ],
                                [
                                    1995,
                                    1995
                                ],
                                [
                                    1996,
                                    1996
                                ],
                                [
                                    1997,
                                    1997
                                ],
                                [
                                    1998,
                                    1998
                                ],
                                [
                                    1999,
                                    1999
                                ],
                                [
                                    2000,
                                    2000
                                ]
                            ],
                            "year_range_empty": [
                                [
                                    1949,
                                    2000
                                ]
                            ]
                        }
                    }
                ]
            }
        ]
    }
]
```



### get all villageId GET POST

http://ngrok.luozm.me:8395/ccvg/advancesearch/get_all_village_id
 

+ GET return first 100 village id
```json
[
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
  ...
    100
]
```
+ POST 

params body json

```json
{
    "begin_num":5,
    "num_range":12
}
```

return beign_num+1(5+1=6) ~ beign_num + num_range(5+12=17)

```json
[
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17
]
```


### download GET

#### Download several villages and all topics http://ngrok.luozm.me:8395/ccvg/advancesearch/download/?village=1137,1145

params **split by comma(,)**

| KEY      | VALUE |
| ----------- | ----------- |
| village      | 1137,1145       |


return

```json
140729100200,翠峰村志,1137,平均温度 Average Yearly Temperature,9.9,°C
140729100200,翠峰村志,1137,平均降水量 Average Yearly Precipitation Amount,520.0,毫米 millimeter
140729100200,翠峰村志,1137,1966,地震 Earthquake
140729100200,翠峰村志,1137,1976,地震 Earthquake
140729100200,翠峰村志,1137,1979,地震 Earthquake
140729100200,翠峰村志,1137,1980,地震 Earthquake
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1975,1975,748.0,人数 number of people
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1976,1976,769.0,人数 number of people
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1977,1977,777.0,人数 number of people
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1978,1978,789.0,人数 number of people
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1981,1981,751.0,人数 number of people
140729100200,翠峰村志,1137,人口 Population,总人口 Total Population,1983,1983,791.0,人数 number of people
...
```

#### Download several villages and one topic http://ngrok.luozm.me:8395/ccvg/advancesearch/download/?village=1137,1145&topic=military
    
params **split by comma(,)**

| KEY      | VALUE |
| ----------- | ----------- |
| village      | 1137,1145      |
| topic      | military      |

***note: only can get one topic***

return

```json
villageId,gazetteerName,gazetteerId,category1,category2,startYear,endYear,data,unit
140729100200,翠峰村志,1137,共产党员 CCP Membership,女 Female,2009,2009,6.0,人数 Number of People
140729100200,翠峰村志,1137,共产党员 CCP Membership,总 Total,2009,2009,33.0,人数 Number of People
140729100200,翠峰村志,1137,共产党员 CCP Membership,男 Male,2009,2009,27.0,人数 Number of People
140429100200,城关村志,1145,共产党员 CCP Membership,女 Female,2009,2009,22.0,人数 Number of People
140429100200,城关村志,1145,共产党员 CCP Membership,总 Total,2009,2009,88.0,人数 Number of People
140429100200,城关村志,1145,共产党员 CCP Membership,男 Male,2009,2009,66.0,人数 Number of People
140429100200,城关村志,1145,阶级成分 Class Status,中农 Middle Peasant,1947,1952,102.0,户数 Number of Households
140429100200,城关村志,1145,阶级成分 Class Status,地主 Landlord,1947,1952,14.0,户数 Number of Households
140429100200,城关村志,1145,阶级成分 Class Status,富农 Rich Peasant,1947,1952,10.0,户数 Number of Households
140429100200,城关村志,1145,阶级成分 Class Status,贫下中农 Poor and Lower Middle Peasant,1947,1952,221.0,户数 Number of Households
```

#### Download one topic and one category http://ngrok.luozm.me:8395/ccvg/advancesearch/download/?village=1137,1145&topic=military&category=阶级成分

params **split by comma(,)**

| KEY      | VALUE |
| ----------- | ----------- |
| village      | 1137       |
| topic      | military  |
| category      | 阶级成分       |

***note: both english and chinese work in category***


| KEY      | VALUE |
| ----------- | ----------- |
| village      | 1137       |
| topic      | military  |
| category      | Class Status       |

return

```json
8,140429100200,城关村志,1145,阶级成分 Class Status,富农 Rich Peasant,1947,1952,10.0,户数 Number of Households
9,140429100200,城关村志,1145,阶级成分 Class Status,贫下中农 Poor and Lower Middle Peasant,1947,1952,221.0,户数 Number of Households
6,140429100200,城关村志,1145,阶级成分 Class Status,中农 Middle Peasant,1947,1952,102.0,户数 Number of Households
7,140429100200,城关村志,1145,阶级成分 Class Status,地主 Landlord,1947,1952,14.0,户数 Number of Households
```

## Utils api
### province GET

+  http://ngrok.luozm.me:8395/ccvg/utils/province

return

```json
[
    "上海市",
    "云南省",
    "内蒙古自治区",
    "北京市",
    "吉林省",
    "四川省",
    "天津市",
    "宁夏回族自治区",
    "安徽省",
    "山东省",
    "山西省",
    "广东省",
    "广西壮族自治区",
    "新疆维吾尔自治区",
    "江苏省",
    "江西省",
    "河北省",
    "河南省",
    "浙江省",
    "海南省",
    "湖北省",
    "湖南省",
    "甘肃省",
    "福建省",
    "贵州省",
    "辽宁省",
    "重庆市",
    "陕西省",
    "青海省",
    "香港特别行政区",
    "黑龙江省"
]
```

### city GET

+  http://ngrok.luozm.me:8395/ccvg/utils/city?province=山东省

params

| KEY      | VALUE |
| ----------- | ----------- |
| province      | 山东省       |

return

```json
[
    "济南市",
    "烟台市",
    "泰安市",
    "德州市",
    "聊城市",
    "菏泽市",
    "日照市",
    "潍坊市",
    "东营市",
    "青岛市",
    "威海市",
    "晋城市",
    "滨州市",
    "临沂市",
    "济宁市",
    "淄博市",
    "枣庄市"
]
```


### county GET

+  http://ngrok.luozm.me:8395/ccvg/utils/city?province=山东省

params

| KEY      | VALUE |
| ----------- | ----------- |
| province      | 山东省       |
| city      | 烟台市       |

return 

```json
[
    "招远市",
    "经济技术开发区",
    "莱州市",
    "技术经济开发区",
    "海阳市",
    "栖霞市",
    "福山区",
    "长岛县",
    "莱山区",
    "龙口市",
    "牟平区"
]
```

### getall GET POST

#### GET
+  http://ngrok.luozm.me:8395/ccvg/utils/getall

return 1500 result

```json
[
    {
        "city": "上海市",
        "county": "奉贤区",
        "id": 19,
        "name": "树园村",
        "province": "上海市"
    },
    {
        "city": "上海市",
        "county": "闵行区",
        "id": 32,
        "name": "井亭村",
        "province": "上海市"
    },
    {
        "city": "上海市",
        "county": "崇明区",
        "id": 39,
        "name": "骏马村",
        "province": "上海市"
    },
  ...
]
```


#### POST 

+ http://ngrok.luozm.me:8395/ccvg/utils/getall

params

**Here can just use only province or only city or only county or only villageName, and they can all combined**

```json
{
    "villageName":"傅山村",
    "county":"张店区",
    "city":"淄博市",
    "province":"山东省"
}
```


return 

```json
[
    {
        "city": "淄博市",
        "county": "张店区",
        "id": 1477,
        "name": "傅山村",
        "province": "山东省"
    }
]
```

