import json
import requests
import pandas as pd
import redshift_connector
from datetime import datetime, timedelta

def lambda_handler(event, context):
    conn = redshift_connector.connect(
    host='ussf-analytics.633549570559.us-east-1.redshift-serverless.amazonaws.com',
    database='ussf_ds',
    port=5439,
    user='mlopez',
    password='Ussf1801!',
    region='us-east-1')

    cursor=conn.cursor()
    conn.autocommit = True

    name='USSF-ds'
    user_key='ABWkzpnyzHPm' # application userkey
    secret_key='CngZPBs9aqXP34SV0QExdOa3n+P5kLoM' #application secretkey
    apikey='3_XnMDjauPbaSvuwLUe2vX7rWnpe87vgfFEou_Hazq271e8t0QequDz2bzV7EYmib1'
    rsa_private_key= '''MIIEowIBAAKCAQEAsQMRllzICo5q6+0CYZzaseHUDUGcS2BkCP5ShGt4y0ULtrEm
    0/s8KM5r4nnpRJO/JVWFTmwf5J79oG22lAGtYniweW87sBtCLHmkVNnWbqdYxbCd
    21c98OgFeqUxar55BnCr+OILH4nrOhOIM/jSUCyGs6L44F4nERkYgOzwyzd2GruM
    s7gHuRBq2s+8OCqKe1J5M0B9vIjxbVXm9Fe7pucpeNM0tvmoBETrCpfzvnyJZ7/i
    6IlMR5JDfjGdyGo8u1hWe3T5YStRfiqF7D8/YkFWjRa9R5gXkzG33HCcYxB6nQs3
    jNdCVbc1ZN/FRIrEZ2ftxYCW/la5G3f2P8fX1wIDAQABAoIBAD+0BITSADX4Whds
    9PHvMq9Yv+lDZv2jg2zPJiA80zyILTaC25/nZxeibiYTzLTe3SgQ9ogqFnI/G7S8
    NzxdvCnXmF8jfl4a+T4rSs40lVy+Qg7nSHAzHfoQbkRcpgOt570/GEzaALnJd0uv
    0tEtZ3buB04nxRxuFe4r02s2mlPvlx2hT3s/KXcDMO+MHnEdtlrJJrf+vM3YQazl
    ASaF5d5DXG7YU9zYrzIVzp/DJ8/VOGPxXmZ8rYzgZKiX+CR8a6BDvw/TV/WhCT9H
    ZDKZXPCynxuT/R7N0ttLhSL8B9/caJmtDtCP/D4mc+pRbPsjtyBkp3oKjCtnYZBu
    ClqVyGECgYEAtapUL36hq607tHaoJ7qnQv0xK7zaFsXh7MgwWgSgcBwW0tgVvXXH
    zXn7GJP+zyytpU1BcvfQBhp3IX2CQ/LDzW1wdNKhVQMBOx0eq5XoJfAnTTP/bVSe
    lgmKhrBMRtUxXpOyHVMRTqeRzhUbzhwzEFaMP2RgMT1FD9bju4ZEMmECgYEA+XFL
    az8e1DMSgtC0xUhPREC03qw/cCUqyBaaWIsQrhRl8xUcGyv61s2ufetzYWr87+7G
    rbk8kLM1luFLHxC9Q+WLdH5KWb2WqDvBZNHapqbc6eKZ14T3+rCvlUOCUPE+T70+
    HR9XFwZ2yX69Cu7fj8S6y2l9SOjsvyV8ejx5JTcCgYAMwD/YdZLzcd1W9V0oLLnA
    wfTJlR/ZqVoKPKLoLpr0Q4R2mCQugC3eBknRl9GyyPgHOjz1zN8VDpZ+C0kxa8DC
    koPqLz6TsPpNI+YbNRJuV0Tq7lNnYSEgdtr6STWRYzVr5gICfOoiEzDwvhqNhP0w
    kGKXsHvv+NT4H6UsbGgtAQKBgBfr6oytb/pvr23GMB/fUGK+Rdo48JiDp/eqW0D0
    jTP0ltZaBukALb0d2ocO5nDIPtuPrZQJeKpz1UbT8k1XYJ87S0VvuxeiG+mkWWae
    zXY/+F2hspk9kj6DZUNORxgWGqCxCFzonYSCxS+LrPt4bw4tkrBhPHeahuHJ6Ycb
    d46/AoGBAKq+8Iak+Dxs+1TDrKboVWGAS+jJAdEEJ2MA23yyT3cRzgcXwVaab1vH
    8iEW7bWljeWJHEoBX0PJt0X4alUqQ2RhKYszj26HzLPwJbuB/0dQAfjnHW1TWMRV
    BkwRqhZuYq1DLnPEhSCTEvfWQNblj9xiFUO13PjGvxLhYU3qLlgP'''
    today = datetime.now()-timedelta(days=1)
    iso_date = today.isoformat()
    query=f'select * from accounts where lastUpdated >= "{iso_date}" limit 10000'
    url='https://accounts.us1.gigya.com/accounts.search'

    params={'UserKey':user_key,
            'secret':secret_key,
            'ApiKey':apikey,
            'query':query,
            'openCursor':'true'}

    response=requests.get(url, params=params)
    df=pd.json_normalize(json.loads(response.text)['results'])
    next_cursor=json.loads(response.text)['nextCursorId']

    alldfs=[df]
    while 'nextCursorId' in json.loads(response.text):
        next_cursor=json.loads(response.text)['nextCursorId']
        print(next_cursor)
        response = requests.get(url, params={'cursorId': next_cursor,
                                             'UserKey':user_key,
                                                'secret':secret_key,
                                                'ApiKey':apikey,})
        df = pd.json_normalize(json.loads(response.text)['results'])
        alldfs.append(df)

    df2=pd.concat(alldfs)

    # convert column types
    date_cols=['lastLogin', 'registered', 'lastUpdated', 'oldestDataUpdated',
               'created', 'verified', 'data.member_since', 'preferences.terms.term1.docDate',
               'preferences.terms.term1.lastConsentModified', 'preferences.privacy.privacy1.lastConsentModified',
               'password.created', 'preferences.terms.term1.actionTimestamp', 'preferences.privacy.privacy1.actionTimestamp',
               'data.membership.subscriptions.end', 'data.membership.subscriptions.begin']
    int_cols=['oldestDataUpdatedTimestamp', 'verifiedTimestamp','lastUpdatedTimestamp','createdTimestamp',
              'registeredTimestamp','lastLoginTimestamp', 'profile.birthDay', 'profile.birthMonth',
              'profile.birthYear', 'profile.age', 'password.hashSettings.rounds']

    df2[int_cols]=df2[int_cols].astype('Int64')
    df2[date_cols]=df2[date_cols].apply(pd.to_datetime)

    # export to s3
    df2.drop(columns='data.membership_history').to_csv('s3://ussfds/gigya/gigya_accounts_update.csv', index=False)

    # unnest caps
    caps=df2[['data.caps', 'UID']].dropna()
    caps2=caps.loc[caps['data.caps'].astype(str)!='[]'].reset_index(drop=True)
    dfs=[]
    for i in range(0, len(caps2)):
        print(i)
        df=pd.json_normalize(caps2['data.caps'][i])
        df['UID']=caps2['UID'][i]
        dfs.append(df)
    caps3=pd.concat(dfs)
    caps3['timestamp']=pd.to_datetime(caps3['timestamp'], errors='coerce')
    caps3[['lat', 'long']]=caps3[['lat', 'long']].apply(pd.to_numeric)
    caps3.to_csv('s3://ussfds/gigya/caps_update.csv', index=False)


    # unnest identites
    identities=df2[['identities', 'UID']].dropna()
    identities2=identities.loc[identities['identities'].astype(str)!='[]'].reset_index(drop=True)
    dfs=[]
    for i in range(0, len(identities2)):
        print(i)
        df=pd.json_normalize(identities2['identities'][i])
        df['UID']=identities2['UID'][i]
        dfs.append(df)
    identities3=pd.concat(dfs)
    int_cols=['birthMonth', 'birthDay', 'birthYear', 'age']
    date_cols=['lastUpdated', 'oldestDataUpdated']
    identities3[int_cols]=identities3[int_cols].astype("Int64")
    identities3[date_cols]=identities3[date_cols].replace('0001-01-01T00:00:00.000Z','').apply(pd.to_datetime)

    identities3.to_csv('s3://ussfds/gigya/identities_update.csv', index=False)

    # ingest into redshift
    column_list=caps3.columns.to_list()
    for i in range(0, len(column_list)):
        if '.' in column_list[i]:
            column_list[i]=f'"{column_list[i]}"'

    column_list2=str(column_list).replace('[', '').replace(']', '').replace("'",'')


    cursor.execute(query)
    cursor.close()
    conn.close()
