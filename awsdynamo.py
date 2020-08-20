#!/usr/bin/env python
# coding: utf-8

# In[ ]:


try:
    import boto3
except:
    get_ipython().system('pip install boto3')
    import boto3
import sys
sys.path.append("/tf/notebooks/libs/")
import S3Lib
from SendSlack import *
print("import finished")


# In[ ]:


servername_param = sys.argv[1]
servername = "[DynamoDB]"

from SendSlack import *
SendSlackSetDefaults("#zolog", servername)
SendSlack("Server Initializing...")


# In[ ]:


session = boto3.Session(aws_access_key_id ='AKIAIKRG45OHHDEOUFMA', aws_secret_access_key ='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', region_name='ap-northeast-2')
print(session)


# In[ ]:


dd = session.resource('dynamodb')
dc = session.client('dynamodb')


# In[ ]:


def IsExistTable(tablename):
    return tablename in dc.list_tables()['TableNames']

def CreateTable(tablename):
    created_table = dd.create_table(TableName=tablename,
                    KeySchema=[
                    {
                        'AttributeName':'id',
                        'KeyType':'HASH'                       
                    }],
                    AttributeDefinitions=
                    [{
                        'AttributeName':'id',
                        'AttributeType':'S'
                    }],
                   ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                   )
    if created_table != None:
        SendSlack('%s Created Table [%s]'%(servername, tablename))
    return created_table

def GetTable(tablename):
    return dd.Table(tablename)

def GetOrCreateTable(tablename):
    if IsExistTable(tablename):
        return GetTable(tablename)
    else:
        return CreateTable(tablename)

def Insert(table, json_dic):
    if type(table) == str:
        table = GetOrCreateTable(table)    
    
    put = table.put_item(Item=json_dic)
    
    result = True
    try:
        result = put['ResponseMetadata']['HTTPStatusCode'] == 200
        if result:
            result = "Succeeded"
        else:
            result = "Failed\n%s"%prettyjson(put)
    except:
        result = "Failed\n%s"%prettyjson(put)
    
    SendSlack('%s\nInsert data : %s\nResult : %s'%(servername, prettyjson(json_dic), result))
    return put

def Get(table, json_dic):
    if type(table) == str:
        if IsExistTable(table):
            table = GetTable(table)
        else:
            return {}
    try:
        response = table.get_item(Key=json_dic)
    except ClientError as e:
        SendSlack("%s GetError : %s"%(servername, e.response['Error']['Message']))
    else:
        item = response['Item']
        return item


# In[ ]:


import json
def prettyjson(uglyjson):
    parsed = uglyjson
    if type(uglyjson) == str:
        parsed = json.loads(uglyjson)    
    return json.dumps(parsed, indent=2, sort_keys=True)


# In[ ]:


table = GetOrCreateTable('temp_table_for_test')
Insert(table, {'id':'test','name':'test'})


# In[ ]:


Get(table, {'id':'test'})


# In[ ]:




