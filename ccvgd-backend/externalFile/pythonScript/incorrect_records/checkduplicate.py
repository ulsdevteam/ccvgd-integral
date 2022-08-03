import csv
import pandas as pd


# with open('/Users/mac/PycharmProjects/CCVG/externalFile/pythonScript/incorrect_records/naturalDisasters_自然灾害.csv', 'r') as f:
#     f_csv = csv.reader(f)
#
#     list_A = []
#     for row in f_csv:
#         list_A.append(row)
#     list_B = list(set(list_A))


data = pd.read_csv("/Users/yifeitai/Desktop/CCVG/externalFile/pythonScript/incorrect_records/economy_经济.csv")
data.drop_duplicates(inplace=True)
file = "/Users/yifeitai/Desktop/CCVG/externalFile/pythonScript/incorrect_records/economy_经济.csv"
data.to_csv(file, index = False)