import json
import boto3
import random
import datetime

kinesis = boto3.client('kinesis')

def getReferrer(item,userId,deviceId):
    data = {}
    data['user_id'] = userId
    data['device_id'] = deviceId
    data['client_event'] = item
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['client_timestamp'] = str_now
    return data
    
def dynamoInsert(sampleRow):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('clicksData')
    table.put_item(Item= sampleRow)

def lambda_handler(event, context):
    #theItem = event["item"]
    
    #zeb
    theItem = event["name"]#zeb
    userId = event["session_id"]#zeb
    deviceId = event["browser"]#zeb
    sampleRow = getReferrer(theItem,userId,deviceId)#zeb
    #zeb
    
    # sampleRow = getReferrer(theItem)
    
    data = json.dumps(sampleRow)
    #insert to dynamoDB
    print("inserting to dynamoDB: ", sampleRow)
    dynamoInsert(sampleRow)

    #put in kinesis streaming
    payload = str(data)+'\n'
    kinesis.put_record(
            StreamName='sessionsclicks',
            Data=payload,
            PartitionKey='partitionkey')
    newtime = datetime.datetime.now()
    print ('Cleaned end execution.')
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

