import pandas as pd
import math
import sys
import numpy as np
from process.validate_csv_data import check_yearly_records, check_range_records, mapping_id


def create_csv_year_of_first_availability_purchase(read_path, output_path, input_file):

    df = pd.read_csv(read_path + "/" + input_file)

    df = df.dropna(axis=0, how='all')

    df.drop_duplicates(inplace=True)


    # create the village_inner_id column by directly copying the "村志代码 Gazetteer Code" column
    df["village_inner_id"] = df["村志代码 Gazetteer Code"]
    # print(df)


    # ---------------csv transpose -------------#
    ### test begin
    df1 = df[["村志代码 Gazetteer Code", "village_inner_id", "液化气 Liquefied Gas"]].copy()
    df1.loc[:, "category"] = '液化气 Liquefied Gas'
    df1 = df1.rename(columns={'液化气 Liquefied Gas': 'data'})


    df2 = df[["村志代码 Gazetteer Code", "village_inner_id", "管道燃气 Pipeline Gas"]].copy()
    df2.loc[:, "category"] = '管道燃气 Pipeline Gas'
    df2 = df2.rename(columns={'管道燃气 Pipeline Gas': 'data'})



    df3 = df[["村志代码 Gazetteer Code", "village_inner_id", "天然气 Natural Gas"]].copy()
    df3.loc[:, "category"] = '天然气 Natural Gas'
    df3 = df3.rename(columns={'天然气 Natural Gas': 'data'})

    df4 = df[["村志代码 Gazetteer Code", "village_inner_id", "自来水 Tap Water"]].copy()
    df4.loc[:, "category"] = '自来水 Tap Water'
    df4 = df4.rename(columns={'自来水 Tap Water': 'data'})

    df5 = df[["村志代码 Gazetteer Code", "village_inner_id", "供电 Electricity Service"]].copy()
    df5.loc[:, "category"] = '供电 Electricity Service'
    df5 = df5.rename(columns={'供电 Electricity Service': 'data'})

    df6 = df[["村志代码 Gazetteer Code", "village_inner_id", "电视机 Television Set"]].copy()
    df6.loc[:, "category"] = '电视机 Television Set'
    df6 = df6.rename(columns={'电视机 Television Set': 'data'})

    df7 = df[["村志代码 Gazetteer Code", "village_inner_id", "电话机 Telephone"]].copy()
    df7.loc[:, "category"] = '电话机 Telephone'
    df7 = df7.rename(columns={'电话机 Telephone': 'data'})

    df8 = df[["村志代码 Gazetteer Code", "village_inner_id", "有线广播 Wired-line Broadcasting"]].copy()
    df8.loc[:, "category"] = '有线广播 Wired-line Broadcasting'
    df8 = df8.rename(columns={'有线广播 Wired-line Broadcasting': 'data'})


    # combine 3 sub-dataframes to one
    df9 = pd.DataFrame(columns=['村志代码 Gazetteer Code', 'village_inner_id', 'data', 'category'])
    size = df["村志代码 Gazetteer Code"].count()
    print(size)

    # print(df1.loc[].tolist())


    count = 0
    for i in range(0, size * 8 + 1, 8):
        try:
            df9.loc[i] = df1.loc[count].tolist()
            df9.loc[i + 1] = df2.loc[count].tolist()
            df9.loc[i + 2] = df3.loc[count].tolist()
            df9.loc[i + 3] = df4.loc[count].tolist()
            df9.loc[i + 4] = df5.loc[count].tolist()
            df9.loc[i + 5] = df6.loc[count].tolist()
            df9.loc[i + 6] = df7.loc[count].tolist()
            df9.loc[i + 7] = df8.loc[count].tolist()
            count += 1
        except Exception as e:
            count += 1
            continue
            print(count)
            print(e)

    # create "level1" column using "category" column
    df9['level1'] = df9['category']
    # print(df9)

    # ---------------csv add Unit column -------------#
    # count = 0  # count number of rows
    # df4["Unit"] = None  # create a new empty column "Unit"
    # unit_temp = []
    # for row in df4['level1'].values.tolist():
    #     if (row == "海拔 (米) Altitude") and (pd.notna(row)):
    #         unit_temp.append("米 Meter")
    #     elif (row == "平均降水量 Average Yearly Precipitation Amount") and (pd.notna(row)):
    #         unit_temp.append("毫米 millimeter")
    #     elif (row == "平均温度 Average Yearly Temperature") and (pd.notna(row)):
    #         unit_temp.append("°C")
    #     else:  # set default unit to number of people
    #         unit_temp.append("没有相关数据 No Available Data")
    # df4["Unit"] = unit_temp
    # print("added unit column to natrual environment yearly table.")

    # ---------------csv using "natrualenvironmentcategory_自然环境类.csv" to code "category_id" column -------------#
    # group by categories
    groupby_categories = df9.groupby(['level1'])

    env_df = pd.DataFrame()
    category_df = pd.read_csv(
        "/Users/yifeitai/Desktop/CCVG/externalFile/pythonScript/Database data/firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类.csv")

    # add "category_id"
    for group, frame in groupby_categories:

        level1_category = group

        parent_id = math.nan
        if level1_category != "":
            category_id = mapping_id(level1_category, parent_id, category_df)
            # parent_id = category_id
            frame['category_id'] = [category_id] * len(frame)

        env_df = pd.concat([env_df, frame])

    # # ---------------create "naturalenvironmentUnit.csv" -------------#
    # # create unit - id mapping
    # units = env_df['Unit'].unique().tolist()
    # # df for naturalenvironmentUnit.csv
    # envUnit = pd.DataFrame({'unit_id': list(range(1, len(units) + 1)), 'name': units})
    #
    # # append unit id
    # env_df = pd.merge(env_df, envUnit, left_on='Unit', right_on='name', how='left')

    # df for naturalEnvironment_自然环境.csv
    naturalenv = pd.DataFrame({'gazetteerId': env_df['村志代码 Gazetteer Code'],
                               'villageInnerId': env_df['村志代码 Gazetteer Code'],
                               'categoryId': env_df['category_id'],
                               # 'unitId': env_df['unit_id'],
                               'data': env_df['data']})

    print("Total {} records for firstAvailabilityorPurchase_第一次购买或拥有年份 table".format(len(naturalenv['gazetteerId'])))
    naturalenv.to_csv(output_path + "/firstAvailabilityorPurchase_第一次购买或拥有年份.csv", index=False, na_rep="NULL")
    # envUnit.to_csv(output_path + "/naturalEnvironmentUnit_自然环境单位.csv", index=False, na_rep="NULL")


    print("Finish firstAvailabilityorPurchase_第一次购买或拥有年份.csv")
    # print("Finish naturalEnvironmentUnit_自然环境单位.csv")