#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import io
sys.path.append("/tf/notebooks/libs/")
#sys.path.append('/tf/notebooks/libs/AWS')
import requests
import AWS.Botosession
import time

import pandas as pd
#슬랙세팅
from SendSlack import *
SendSlackSetDefaults("#s3_log", "s3-logger")


# In[ ]:


AWS_REGION_NAME = 'ap-northeast-2'
AWS_ATHENA_RESULT_BUCKETNAME = 'aws-athena-query-results-auto-ap-northeast-2'

def Query(dbname, query):
    '''
    dbname : aws 데이터 카탈로그 이름
    query : 쿼리명
    리턴값 : 데이터프레임
    
    주의
    1. exception 일 경우 대부분은 쿼리를 제대로 실행하지 못했을 경우임
    2. 쿼리 result 는 aws-athena-query-results-auto-ap-northeast-2 버킷에 저장됨 (csv)
    '''
    global AWS_REGION_NAME
    global AWS_ATHENA_RESULT_BUCKETNAME
    
    session = AWS.Botosession.session()
    credentials = session.get_credentials()
    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    athena = session.client('athena')
    bucketname = AWS_ATHENA_RESULT_BUCKETNAME
    athenaquery_str = query
    SendSlack("[S3 Download] begin query to athena : %s"%athenaquery_str)
    queryStart = athena.start_query_execution(
        QueryString = athenaquery_str,
        QueryExecutionContext = {
            'Database': dbname
        },
        ResultConfiguration={
            'OutputLocation':'s3://%s/'%bucketname
        }
    )
    query_id = queryStart['QueryExecutionId']
    while True:
        qs = athena.get_query_execution(QueryExecutionId=query_id)
        status = qs['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)  # 200ms
    SendSlack("finish athena query")
    SendSlack("get result from s3 with %s.csv"%query_id)
    s3r = session.resource('s3')
    file = s3r.Object(bucketname, "%s.csv"%query_id)
    f = file.get()['Body'].read()
    SendSlack("finish get result ... making dataframe")
    df = pd.read_csv(io.BytesIO(f))
    SendSlack("[S3 Download] finish make dataframe with rows = %d"%len(df))
    return df


# In[ ]:





# In[ ]:


def MakeDateWhere(where_column, str_startdate, str_enddate, datetimeformat='%Y-%m-%d', step_seconds=86400):
    import datetime
    dbegin = datetime.datetime.strptime(str_startdate, datetimeformat)
    dend = datetime.datetime.strptime(str_enddate, datetimeformat)
    step = datetime.timedelta(seconds=step_seconds)
    
    lsdate = []
    while dbegin <= dend:
        lsdate.append(dbegin.strftime(datetimeformat))
        dbegin += step    
    if str_enddate not in lsdate:
        lsdate.append(str_enddate)        
    
    return " (" + " or ".join([f"\"{where_column}\" = '{d}'" for d in lsdate]) + ") "


# In[ ]:


if __name__ == "__main__":
    print(MakeDateWhere('partition_0', '2020-03-23', '2020-04-02'))


# In[ ]:


if __name__ == "__main__":
    display(Query('applodata', 'select * from "applodata";'))


# In[ ]:





# In[ ]:




