import os
import io
import boto3
import json
import csv

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    data = json.loads(json.dumps(event))
    payload = data['data']
    #name = data['name']
    print("payload: ",payload)
    
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='text/csv',
                                       Body=payload)
    print("response: ",response)
    result = json.loads(response['Body'].read().decode())
    print("result: ",result)
    #round pred to 0 or 1
    if(result>0.5):
        pred = "Reachable University"
    else:
        pred = "Hard to get into"
    print("pred: ", pred)
    predicted_label = data['name']+" :"+ pred 
    
    return predicted_label
    # return "hello"

"""
response_body = {"data":"380,3.61,3"} #gre,gpa,rank
{"data":"380,3.61,3"}
{"data":"300,2.8,1"}

endpoint: xgboost-2019-12-21-07-06-31-136
"""
