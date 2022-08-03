import math
from pythonScript.process.validate_csv_data import mapping_id
import pandas as pd
read_csv_dirct_yearly = "/Users/mac/PycharmProjects/CCVG/externalFile/pythonScript/All Datasets and Data Dictionary/Military, Politics and Management - Yearly.csv"
read_csv_dirct_range = "/Users/mac/PycharmProjects/CCVG/externalFile/pythonScript/All Datasets and Data Dictionary/Military, Politics and Management - Range.csv"
output_path = "/Users/mac/PycharmProjects/CCVG/externalFile"

# process.create_csv_military_militaryUnit(read_csv_dirct, output_csv_dirct, yearly_file="Military, Politics and Management - Yearly.csv", range_file="Military, Politics and Management - Range.csv")

def military_militaryUnit( yearly_file="Military, Politics and Management - Yearly.csv", range_file="Military, Politics and Management - Range.csv"):
    yearly_df = pd.read_csv(read_csv_dirct_yearly)
    range_df = pd.read_csv(read_csv_dirct_range)

    yearly_df = yearly_df.dropna(axis=0, how='all')
    range_df = range_df.dropna(axis=0, how='all')

    yearly_df.drop_duplicates(inplace=True)
    range_df.drop_duplicates(inplace=True)


    for column in ['Division1', 'Division2']:
        yearly_df[column] = yearly_df[column].where(yearly_df[column].notnull(), "")
        range_df[column] = range_df[column].where(range_df[column].notnull(), "")

    yearly_df['level1'] = yearly_df['Category']
    yearly_df['level2'] = yearly_df['Division1'] + yearly_df['Division2']

    range_df['level1'] = range_df['Category']
    range_df['level2'] = range_df['Division1'] + range_df['Division2']

    # add "Unit" column to the yearly_df
    yearly_df["Unit"] = None  # create a new empty column "Unit"
    unit_temp = []
    for row in yearly_df['level1'].values.tolist():
        if row == "村民纠纷 Number of Civil Mediations":
            unit_temp.append("解决件数 Number of Resolved Mediations")
        elif row == "共产党员 CCP Membership":
            unit_temp.append("人数 Number of People")
        elif row == "阶级成分 Class Status":
            unit_temp.append("户数 Number of Households")
        elif row == "入伍 Military Enlistment":
            unit_temp.append("人数 Number of People")
        elif row == "新党员 New CCP Membership":
            unit_temp.append("人数 Number of People")
        elif row == "刑事案件 Number of Reported Crimes":
            unit_temp.append("发生件数 Number of Cases Happened")
        else:  # set default unit to number of people
            unit_temp.append("人数 Number of People")
    yearly_df["Unit"] = unit_temp
    print("added unit column to military yearly table.",yearly_df)

    count = 0  # count number of rows
    range_df["Unit"] = None  # create a new empty column "Unit"
    unit_temp = []
    for row in range_df['level1'].values.tolist():
        if row == "村民纠纷 Number of Civil Mediations":
            unit_temp.append("解决件数 Number of Resolved Mediations")
        elif row == "共产党员 CCP Membership":
            unit_temp.append("人数 Number of People")
        elif row == "阶级成分 Class Status":
            unit_temp.append("户数 Number of Households")
        elif row == "入伍 Military Enlistment":
            unit_temp.append("人数 Number of People")
        elif row == "新党员 New CCP Membership":
            unit_temp.append("人数 Number of People")
        elif row == "刑事案件 Number of Reported Crimes":
            unit_temp.append("发生件数 Number of Cases Happened")
        else:  # set default unit to number of people,should not work
            unit_temp.append("人数 Number of People")
    range_df["Unit"] = unit_temp

    print("added unit column to population range_df table.",range_df)

    # transfer yearly_df to dictionary
    yearly_data = {}
    for column in yearly_df.columns:
        yearly_data[column] = yearly_df[column].values.tolist()

    # transfer range_df to dictionary
    range_data = {}
    for column in range_df.columns:
        range_data[column] = range_df[column].values.tolist()

    # merge yearly_df and range_df into df_yearly_range
    yearly_and_range = {
        '村志代码 Gazetteer Code': [],
        'level1': [],
        'level2': [],
        'Start Year': [],
        'End Year': [],
        'Data': [],
        'Unit': []}

    print("process {} records in {} file".format(len(yearly_data['村志代码 Gazetteer Code']), yearly_file))
    # select and store not null records at yearly
    for i in range(len(yearly_data['村志代码 Gazetteer Code'])):
        for year in range(1949, 2020):
            # skip null records
            if str.isdigit(str(yearly_data[str(year)][i])):
                continue
                # store gazetteer code, categories, unit
            for key in ['村志代码 Gazetteer Code', 'level1', 'level2', 'Unit']:
                yearly_and_range[key].append(yearly_data[key][i])
            # store data
            yearly_and_range['Data'].append(yearly_data[str(year)][i])
            # store start year, end year
            yearly_and_range['Start Year'].append(year)
            yearly_and_range['End Year'].append(year)

    print("process {} records in {} file".format(len(range_data['村志代码 Gazetteer Code']), range_file))
    # store range records
    for i in range(len(range_data['村志代码 Gazetteer Code'])):
        # store gazetteer code, categories, unit, start year, end year, data
        for key in ['村志代码 Gazetteer Code', 'level1', 'level2', 'Start Year', 'End Year', 'Data', 'Unit']:
            yearly_and_range[key].append(range_data[key][i])

    # create df stores yearly and range data
    yearly_and_range_df = pd.DataFrame(yearly_and_range)

    # --- append category id ---
    # group by categories
    groupby_categories = yearly_and_range_df.groupby(['level1', 'level2'])

    population_df = pd.DataFrame()
    category_df = pd.read_csv("/Users/mac/PycharmProjects/CCVG/externalFile/pythonScript/tempmilitarycategory_军事类.csv")

    for group, frame in groupby_categories:

        level1_category = group[0]
        level2_category = group[1]

        parent_id = math.nan
        if level1_category != "":
            category_id = mapping_id(level1_category, parent_id, category_df)
            parent_id = category_id
            frame['category_id'] = [category_id] * len(frame)

        if level2_category != "":
            category_id = mapping_id(level2_category, parent_id, category_df)
            parent_id = category_id
            frame['category_id'] = [category_id] * len(frame)

        population_df = pd.concat([population_df, frame])

    # create unit - id mapping
    units = population_df['Unit'].unique().tolist()
    # df for economyCategory_经济类.csv
    economyUnit = pd.DataFrame({'unit_id': list(range(1, len(units) + 1)), 'name': units})

    # append unit id
    population_df = pd.merge(population_df, economyUnit, left_on='Unit', right_on='name', how='left')

    # df for economy_经济.csv
    economy = pd.DataFrame({'gazetteerId': population_df['村志代码 Gazetteer Code'],
                            'villageInnerId': population_df['村志代码 Gazetteer Code'],
                            'categoryId': population_df['category_id'],
                            'startYear': population_df['Start Year'],
                            'endYear': population_df['End Year'],
                            'unitId': population_df['unit_id'],
                            'data': population_df['Data']})

    print("Total {} records for military_军事 table".format(len(economy['gazetteerId'])))

    economy.to_csv(output_path + "/military_军事.csv", index=False, na_rep="NULL")
    economyUnit.to_csv(output_path + "/militaryUnit_军事单位.csv", index=False, na_rep="NULL")

    print("Finish military_军事.csv")
    print("Finish /militaryUnit_军事单位.csv")

if __name__ == "__main__":
    military_militaryUnit()