import pandas as pd
import numpy as np
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def engine_generator():#generating engine string to push to postgresql server
    password='walrus312'
    engine_string=f'postgresql://postgres:{password}@localhost:5432/completions'
    #engine_string = f"postgresql://'{username}:{password}@{host_address}:{port}/{database_name}"
    engine = create_engine(engine_string, echo=False)
    if not database_exists(engine.url):
            create_database(engine.url)
    return engine.connect()
def dataload(connection, table_name, df):
  df.to_sql(table_name, connection, if_exists='replace', chunksize=1000)
#cleaning for unitid index
def unit_id_cleaner(unitid_index,state_code_index,location_list):

    unitid_index.drop(columns=['FTE','(IPEDS)'],inplace=True)
    unitid_index['Unit ID'].fillna('bad_row',inplace=True)
    unitid_index['Year Type'].fillna('missing',inplace=True)
    unitid_index['Organization or School Name'].fillna('missing',inplace=True)
    for i in range(len(unitid_index)):
        if unitid_index.iloc[i]['Unit ID']=='bad_row':#fixes rows where the school name was put into the row above
                unitid_index.iloc[(i+1), unitid_index.columns.get_loc('Organization or School Name')] =\
                                                                    unitid_index.iloc[i, unitid_index.columns.get_loc('Organization or School Name')]+\
                                                                    unitid_index.iloc[(i+1), unitid_index.columns.get_loc('Organization or School Name')]
        elif unitid_index.iloc[i]['Year Type']=='missing':#fixes rows where the year type was put into the school name 
            unitid_index.iloc[i, unitid_index.columns.get_loc('Year Type')]=unitid_index.iloc[i, unitid_index.columns.get_loc('Organization or School Name')][-6:]
            unitid_index.iloc[i, unitid_index.columns.get_loc('Organization or School Name')]=\
                                                                    unitid_index.iloc[i, unitid_index.columns.get_loc('Organization or School Name')][:-7]
        elif str(unitid_index.iloc[i]['Organization or School Name'])[-4:]=="UNY)":#get rid of the (cuny) and (suny) suffixes for wikipedia scrape
            unitid_index.iloc[(i), unitid_index.columns.get_loc('Organization or School Name')]=\
                                                                    unitid_index.iloc[(i), unitid_index.columns.get_loc('Organization or School Name')][:-7]
        elif str(unitid_index.iloc[i]['Organization or School Name'])[-1:]==")":#changing the state abbreviations to proper names for wikipedia scrape
            for a in range(len(state_code_index)):
                if unitid_index.iloc[i]['Organization or School Name'][-3:-1]==state_code_index.iloc[a]['Abbreviation']:
                        unitid_index.iloc[(i), unitid_index.columns.get_loc('Organization or School Name')]=\
                                                                    unitid_index.iloc[(i), unitid_index.columns.get_loc('Organization or School Name')][:-3]\
                                                                    +state_code_index.iloc[a, state_code_index.columns.get_loc('State')]+')'
        elif unitid_index.iloc[i]['Organization or School Name']=='missing':#deal with the rows that have the school name in the unitid
            unitid_index.iloc[i, unitid_index.columns.get_loc('Organization or School Name')]=unitid_index.iloc[i, unitid_index.columns.get_loc('Unit ID')][9:]
            unitid_index.iloc[i, unitid_index.columns.get_loc('Unit ID')]=unitid_index.iloc[i, unitid_index.columns.get_loc('Unit ID')][:8]
        unitid_index.iloc[i]['Organization or School Name']=unitid_index.iloc[i]['Organization or School Name'].replace('-','_')#replacing the - with underscore for wiki scrape
    unitid_index=unitid_index[unitid_index['Unit ID']!='bad_row']#deleteing the rows where there is only an incomplete school name
    unitid_index['Unit ID']=unitid_index['Unit ID'].astype('Int64')
    unitid_index=pd.merge(unitid_index,location_list,'left',left_on='Unit ID',right_on='schoolid')
    return unitid_index

#changing column names to be more human readable
def df_creator(csv_dict,award_code_index,cip_code_index,unitid_index,demographics,earliest_year,latest_year):
    for i in range(earliest_year,latest_year):
        print(i)
        csv_dict[f'{i}']=csv_dict[f'{i}'].rename(columns=
                                                {
                                                    'UNITID':'school_id',
                                                    'CIPCODE':'major_id',
                                                    'MAJORNUM':'first_or_second_major',
                                                    'AWLEVEL':'degree_level_code',
                                                    'CTOTALT':'total_degrees',
                                                    'CTOTALM':'total_degrees_male',
                                                    'CTOTALW':'total_degrees_female',
                                                    'CAIANT':'american_indian_or_alaska_native_total',
                                                    'CAIANM':'american_indian_or_alaska_native_total_male',
                                                    'CAIANW':'american_indian_or_alaska_native_total_female',
                                                    'CASIAT':'asian_total',
                                                    'CASIAM':'asian_total_male',
                                                    'CASIAW':'asian_total_female',
                                                    'CBKAAT':'black_or_african_american_total',
                                                    'CBKAAM':'black_or_african_american_male',
                                                    'CBKAAW':'black_or_african_american_female',
                                                    'CHISPT':'hispanic_or_latino_total',
                                                    'CHISPM':'hispanic_or_latino_male',
                                                    'CHISPW':'hispanic_or_latino_female',
                                                    'CNHPIT':'native_hawaiian_or_other_pacfic_islander_total',
                                                    'CNHPIM':'native_hawaiian_or_other_pacfic_islander_male',
                                                    'CNHPIW':'native_hawaiian_or_other_pacfic_islander_female',
                                                    'CWHITT':'white_total',
                                                    'CWHITM':'white_total_male',
                                                    'CWHITW':'white_total_female',
                                                    'C2MORT':'two_or_more_races_total',
                                                    'C2MORM':'two_or_more_races_total_male',
                                                    'C2MORW':'two_or_more_races_total_female',
                                                    'CUNKNT':'race/ethnicity_unknown_total',
                                                    'CUNKNM':'race/ethnicity_unknown_male',
                                                    'CUNKNW':'race/ethnicity_unknown_female',
                                                    'CNRALT':'nonresident_alien_total',
                                                    'CNRALW  ':'nonresident_alien_total_female',
                                                    'CNRALM':'nonresident_alien_total_male',
                                                    'XCTOTALT':'total_degrees_collection_method',
                                                    'XCTOTALM':'total_degrees_male_collection_method',
                                                    'XCTOTALW':'total_degrees_female_collection_method',
                                                    'XCAIANT':'american_indian_or_alaska_native_total_collection_method',
                                                    'XCAIANM':'american_indian_or_alaska_native_total_male_collection_method',
                                                    'XCAIANW':'american_indian_or_alaska_native_total_female_collection_method',
                                                    'XCASIAT':'asian_total_collection_method',
                                                    'XCASIAM':'asian_total_male_collection_method',
                                                    'XCASIAW':'asian_total_female_collection_method',
                                                    'XCBKAAT':'black_or_african_american_total_collection_method',
                                                    'XCBKAAM':'black_or_african_american_male_collection_method',
                                                    'XCBKAAW':'black_or_african_american_female_collection_method',
                                                    'XCHISPT':'hispanic_or_latino_total_collection_method',
                                                    'XCHISPM':'hispanic_or_latino_male_collection_method',
                                                    'XCHISPW':'hispanic_or_latino_female_collection_method',
                                                    'XCNHPIT':'native_hawaiian_or_other_pacfic_islander_total_collection_method',
                                                    'XCNHPIM':'native_hawaiian_or_other_pacfic_islander_male_collection_method',
                                                    'XCNHPIW':'native_hawaiian_or_other_pacfic_islander_female_collection_method',
                                                    'XCWHITT':'white_total_collection_method',
                                                    'XCWHITM':'white_total_male_collection_method',
                                                    'XCWHITW':'white_total_female_collection_method',
                                                    'XC2MORT':'two_or_more_races_total_collection_method',
                                                    'XC2MORM':'two_or_more_races_total_male_collection_method',
                                                    'XC2MORW':'two_or_more_races_total_female_collection_method',
                                                    'XCUNKNT':'race/ethnicity_unknown_total_collection_method',
                                                    'XCUNKNM':'race/ethnicity_unknown_male_collection_method',
                                                    'XCUNKNW':'race/ethnicity_unknown_female_collection_method',
                                                    'XCNRALT':'nonresident_alien_total_collection_method',
                                                    'XCNRALM':'nonresident_alien_total_male_collection_method',
                                                    'XCNRALW':'nonresident_alien_total_female_collection_method'
                                                })
        csv_dict[f'{i}']=csv_dict[f'{i}'][csv_dict[f'{i}']['major_id']!=99]
        #getting rid of nulls and duplicates
        csv_dict[f'{i}'].dropna(axis=0,inplace=True)
        csv_dict[f'{i}'].drop_duplicates(inplace=True)
        #joining all of the csv together to have all of the csvs in one place 
        csv_dict[f'{i}']=pd.merge(
                            pd.merge(
                                pd.merge(
                                    pd.merge(csv_dict[f'{i}'],
                                            award_code_index,
                                            'inner',
                                            left_on='degree_level_code',
                                            right_on='Award_level').drop(columns=['Award_level']),
                                        cip_code_index,
                                        'inner',
                                        left_on='major_id',
                                        right_on='codevalue').drop(columns=['codevalue']),
                                    unitid_index,
                                    'left',
                                    left_on='school_id',
                                    right_on='Unit ID'
                                ).drop(columns=['Unit ID']),demographics,'left',left_on='states',right_on='State')
        csv_dict[f'{i}']=csv_dict[f'{i}'].rename(columns={
                                                            'Organization or School Name':'school_name',
                                                            'Year Type':'year_type'
                                                            })
        # filling the row with school id not in the unitid index
        csv_dict[f'{i}']['school_name'].fillna('no matching id',inplace=True)
        csv_dict[f'{i}']['year_type'].fillna('no matching id',inplace=True)
        csv_dict[f'{i}']['longitudes'].fillna(0,inplace=True)
        csv_dict[f'{i}']['latitudes'].fillna(0,inplace=True)
        #adding in a year column and a unique key for each row 
        csv_dict[f'{i}']['year']=i
        csv_dict[f'{i}']['key']=csv_dict[f'{i}']['school_id'].astype(str)+\
                                '-'+(csv_dict[f'{i}']['major_id']).astype(str)+\
                                    '-'+(csv_dict[f'{i}']['year']).astype(str)
        csv_dict[f'{i}'].to_csv(f'./datasets/{i}.csv')
    return csv_dict
def disaggregator(csv_dict,earliest_year,latest_year):
    for b in range(earliest_year,latest_year):
        csv_dict[f'{b}']=csv_dict[f'{b}'][csv_dict[f'{b}']['Award_name']=="bachelor's degree"]
        print(b)
        disaggregated=pd.DataFrame()
        dataframes=[]
        gender=[]
        ethnicity=[]
        for a in range(len(csv_dict[f'{b}'])):
            bucket=csv_dict[f'{b}'].iloc[a][[
                                            'school_id',
                                             'major_id',
                                             'first_or_second_major',
                                             'Award_name',
                                             'valuelabel',
                                             'school_name',
                                             'year_type',
                                             'longitudes',
                                             'latitudes',
                                             'year',
                                             'states',
                                             'Miscellaneous.Percent Female',
                                             'Ethnicities.Black Alone',
                                             'Ethnicities.American Indian and Alaska Native Alone',
                                             'Ethnicities.Asian Alone',
                                             'Ethnicities.Native Hawaiian and Other Pacific Islander Alone',
                                             'Ethnicities.Two or More Races',
                                             'Ethnicities.White Alone Not Hispanic',
                                             'Ethnicities.Hispanic or Latino']]
            for i in range(csv_dict[f'{b}'].iloc[a]['race/ethnicity_unknown_male']):
                gender.append('Male')
                ethnicity.append('Race/Ethnicity Unknown')
            for i in range(csv_dict[f'{b}'].iloc[a]['race/ethnicity_unknown_female']):
                gender.append('Female')
                ethnicity.append('Race/Ethnicity Unknown')
            for i in range(csv_dict[f'{b}'].iloc[a]['american_indian_or_alaska_native_total_male']):
                gender.append('Male')
                ethnicity.append('American Indian or Alaska Native')
            for i in range(csv_dict[f'{b}'].iloc[a]['american_indian_or_alaska_native_total_female']):
                gender.append('Female')
                ethnicity.append('American Indian or Alaska Native')
            for i in range(csv_dict[f'{b}'].iloc[a]['asian_total_male']):
                gender.append('Male')
                ethnicity.append('Asian')
            for i in range(csv_dict[f'{b}'].iloc[a]['asian_total_female']):
                gender.append('Female')
                ethnicity.append('Asian')
            for i in range(csv_dict[f'{b}'].iloc[a]['black_or_african_american_male']):
                gender.append('Male')
                ethnicity.append('Black or African American')
            for i in range(csv_dict[f'{b}'].iloc[a]['black_or_african_american_female']):
                gender.append('Female')
                ethnicity.append('Black or African American')
            for i in range(csv_dict[f'{b}'].iloc[a]['hispanic_or_latino_male']):
                gender.append('Male')
                ethnicity.append('Hispanic/Latino')
            for i in range(csv_dict[f'{b}'].iloc[a]['hispanic_or_latino_female']):
                gender.append('Female')
                ethnicity.append('Hispanic/Latino')
            for i in range(csv_dict[f'{b}'].iloc[a]['native_hawaiian_or_other_pacfic_islander_male']):
                gender.append('Male')
                ethnicity.append('Native Hawaiian or Other Pacific Islander')
            for i in range(csv_dict[f'{b}'].iloc[a]['native_hawaiian_or_other_pacfic_islander_female']):
                gender.append('Female')
                ethnicity.append('Native Hawaiian or Other Pacific Islander')
            for i in range(csv_dict[f'{b}'].iloc[a]['white_total_male']):
                gender.append('Male')
                ethnicity.append('White')
            for i in range(csv_dict[f'{b}'].iloc[a]['white_total_female']):
                gender.append('Female')
                ethnicity.append('White')
            for i in range(csv_dict[f'{b}'].iloc[a]['two_or_more_races_total_male']):
                gender.append('Male')
                ethnicity.append('Two or More Races')
            for i in range(csv_dict[f'{b}'].iloc[a]['two_or_more_races_total_female']):
                gender.append('Female')
                ethnicity.append('Two or More Races')
            for i in range(csv_dict[f'{b}'].iloc[a]['nonresident_alien_total_male']):
                gender.append('Male')
                ethnicity.append('Nonresident Alien')
            for i in range(csv_dict[f'{b}'].iloc[a]['nonresident_alien_total_female']):
                gender.append('Female')
                ethnicity.append('Nonresident Alien')
            temp=pd.DataFrame(np.repeat(bucket.to_frame().values,csv_dict[f'{b}'].iloc[a]['total_degrees'],axis=1))
            temp=temp.T
            temp.columns=bucket.index
            dataframes.append(temp)
            print(a)
        disaggregated=pd.concat(dataframes)
        disaggregated['gender']=gender
        disaggregated['ethnicity']=ethnicity
        csv_dict[f'{b}']=disaggregated
    return csv_dict
def main():
    earliest_year=2017#change to include earlier years
    latest_year=2022#to change to lastest year change to latest year+1

    #reading in the csvs
    unitid_index=pd.read_csv('./datasets/unitid_index.csv')
    state_code_index=pd.read_csv('./datasets/states.csv')
    cip_code_index=pd.read_csv('./datasets/cipcodeindex.csv')
    cip_code_index=cip_code_index[['codevalue','valuelabel']]
    award_code_index=pd.read_csv('./datasets/award_code_index.csv')
    location_list=pd.read_csv('./datasets/location_info.csv')
    demographics=pd.read_csv('./datasets/state_demographics.csv')
    csv_names=[]
    csvs=[]
    for i in range(earliest_year,latest_year):
        csv_names.append(f'{i}')
        csvs.append(pd.read_csv(f'./datasets/c{i}_a_rv.csv'))
    csv_dict=dict(zip(csv_names,csvs))
    unitid_index=unit_id_cleaner(unitid_index,state_code_index,location_list)
    
    #adding in longs and lats from wikiscrape
    

    
    csv_dict=df_creator(csv_dict,award_code_index,cip_code_index,unitid_index,demographics,earliest_year,latest_year)

    csv_dict=disaggregator(csv_dict,earliest_year,latest_year)

    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)
    for i in range(earliest_year,latest_year):#pushing the tables to postgresql server
        dataload(engine_generator(),f'comp_{i}',csv_dict[f'{i}'])
        engine_generator().close()
main()

