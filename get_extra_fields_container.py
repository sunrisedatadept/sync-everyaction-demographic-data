
#!/usr/bin/env python
# coding: utf-8


# load the necessary packages
import pandas as pd
import numpy as np
from parsons import Redshift, Table, VAN, S3, utilities
from datetime import date, datetime
from requests.exceptions import HTTPError
import os
import logging

#CIVIS enviro variables
os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']
van_key = os.environ['VAN_PASSWORD']

#init Redshift instance
rs = Redshift()
ea = VAN(db = 'EveryAction', api_key = van_key)


# table = rs.query('select contacts.vanid from sunrise_ea.tsm_tmc_contacts_sm contacts limit 500')
table = rs.query("""
                with max_date as (
                  select contacts.vanid
                  , max(datemodified) as max_datemod
                  , max(date_updated) as max_dateup
                  from sunrise_ea.tsm_tmc_contacts_sm contacts
                  left join sunrise.contacts_extra_fields fields
                  ON contacts.vanid = fields.vanid
                  group by 1
                  )
              select vanid
              from max_date
              where max_datemod > max_dateup
              or max_dateup is null
              limit 10000
                """)

# Table variable changed to panda dataframe which then transfers to list 
list_vanid = table.to_dataframe()['vanid'].to_list()

# init dict with variables for final table| create 
result_dict = {} 
# Create empty lists where custom fields will live if found 

result_dict['vanid'] = []
result_dict['dob'] = []
result_dict['race'] = []
result_dict['gender'] = []
result_dict['class'] = []
result_dict['active'] = []

# Creating empty lists where we will log successes and errors
successes = []
errors = []

# For every vanid in the list pulled from the SELECT statement above:
# Check if the response is a dictionary to avoid throwing Civis an error (in the case of a bad vanid where the result is NONE)
# Then parse the JSON dump to get vanid, dob, all of the races and genders the user selected, class, hub, and hub role.

for person in list_vanid:
    try:
        response = ea.get_person(person, id_type='vanid', expand_fields = ['reported_demographics','custom_fields'])
    except HTTPError as e:
        response = e

    applied_at = str(datetime.now())

    if type(response) == dict:
        result_dict['vanid'].append(response['vanId'])
        result_dict['dob'].append(response['dateOfBirth'])
        race_list = []

        if response['selfReportedRaces'] is not None:
            for race in response['selfReportedRaces']:
                race_list.append(race['reportedRaceName'])
        else:
            race_list.append(None)
        result_dict['race'].append(race_list)  

        gender_list = []
        if response['selfReportedGenders'] is not None:
            for gender in response['selfReportedGenders']:
                gender_list.append(gender['reportedGenderName'])
        else:
            gender_list.append(None)
        result_dict['gender'].append(gender_list)

        result_dict['class'].append([item for item in response['customFields'] if item["customFieldId"] == 19][0]['assignedValue'])
        result_dict['active'].append([item for item in response['customFields'] if item["customFieldId"] == 6][0]['assignedValue'])
      
    # If the update succeeds it will append - we put relevant info into the successes log
    if isinstance(response, dict):
                success = {
                    "vanid": response.pop('vanId'),
                    "applied_at": applied_at,
                    "list_vanid": result_dict
                }
                successes.append(success)
    else:
        # Save error data if anything goes wrong
        error = {
            "errored_at": applied_at,
            "vanid": person,
            "error": str(response)[:999]
        }
        errors.append(error)
        # End loop
         

# clean dataframe
result_df = pd.DataFrame(result_dict)
result_df['dob'] = result_df['dob'].astype(str).str[0:10]
result_df['race'] = result_df['race'].apply(lambda x: ','.join(map(str, x)))
result_df['gender'] = result_df['gender'].apply(lambda x: ','.join(map(str, x)))


# create a list of available values for each demogrpahic var using Parsons van.get_custom_field() 
active_list = ea.get_custom_field(6)['availableValues']
class_list = ea.get_custom_field(19)['availableValues']

# convert list to dictionary to map to dataframe - 
#this separates the custome fields in their own lists

class_dict = {}
for d in class_list:
    class_dict[str(d['id'])] = d['name']


# map values of class, hub, and hub role to dataframe
result_df['class'].replace(class_dict, inplace = True)
result_df['date_updated'] = date.today()

result_df = result_df.where(pd.notnull(result_df), None)
result_df = result_df.replace({'None': None})
result_df = result_df.replace({np.nan: None})

# col order same as civis
result_df= result_df[['vanid', 'dob', 'race', 'gender', 'class', 'active', 'date_updated']]

# Overwrite EA abckend terms with Sunrise's preferred front end terms
# Asian -> Asian/Asian American
# Black or African American -> Black/African American
# Caucasian or White -> Caucasian/White
# Hispanic -> Latino/Latina/Latinx
# Middle Eastern -> Middle Eastern
# Native American -> Native American/First Nations/Alaska Native
# Native Hawaiian -> Native Hawaiian
# Pacific Islander -> Pacific Islander
# Other -> Other

result_df['race'] = result_df['race'].str.replace('Asian','Asian/Asian American')
result_df['race'] = result_df['race'].str.replace('Black or African American','Black/African American')
result_df['race'] = result_df['race'].str.replace('Caucasian or White','Caucasian/White')
result_df['race'] = result_df['race'].str.replace('Hispanic','Latino/Latina/Latinx')
result_df['race'] = result_df['race'].str.replace('Native American','Native American/First Nations/Alaska Native')

# convert data frame to Parsons Table
result_table = Table.from_dataframe(result_df)

# copy Table into Redshift, append new rows
rs.copy(result_table, 'sunrise.contacts_extra_fields' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)
