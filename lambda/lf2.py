from __future__ import print_function
import boto3
import base64
import json
  
ddb = boto3.client('dynamodb')
fh = boto3.client('firehose')
cw = boto3.client('cloudwatch')
  
  
def save_logs_in_cloudwatch(cw_write): 
    REGION='us-east-1'
    Unit=cw_write['parameter']
    ValueCW=cw_write['events']
    MetricName = cw_write['metricname']
    response = cw.put_metric_data(
            Namespace='sessionization',
            MetricData=[
                {
                    'MetricName': MetricName,
                    'Dimensions': [
                        {
                            'Name': 'region',
                            'Value': REGION
                        },
                    ],
                    'Value': ValueCW,
                    'Unit': Unit,
                    'StorageResolution': 1
                },
            ]
    )

def lambda_handler(event, context):
  count = 0
  for record in event['records']:
      message = base64.b64decode(record['data'])
      msn = json.loads(message)
      document = {'session_id': msn['SESSION_ID'], 'user_id': msn['USER_ID'], 'device_Id': msn['DEVICE_ID'],'timeagg': msn['TIMEAGG'],
                  'events': msn['EVENTS'],'beginnavigation': msn['BEGINNAVIGATION'],'endnavigation': msn['ENDNAVIGATION'],
                  'beginsession' : msn['BEGINSESSION'], 'endsession' : msn['ENDSESSION'], 'duration_sec': msn['DURATION_SEC']
      }
      documentjson = str(document) + '\n'
      fh.put_record(DeliveryStreamName='sessionanalytics-FIrehoseToS3-1DN7SOPTPZVFN',Record={ 'Data': documentjson})
      cw_write = {
                 'events' : msn['DURATION_SEC'],
                 'parameter' : 'Seconds',
                 'metricname' : 'session_duration',
             }
     REGION='us-east-1'
     Unit=cw_write['parameter']
     ValueCW=cw_write['events']
     MetricName = cw_write['metricname']
     response = cw.put_metric_data(
             Namespace='sessionization',
             MetricData=[
                 {
                     'MetricName': MetricName,
                     'Dimensions': [
                         {
                             'Name': 'region',
                             'Value': REGION
                         },
                     ],
                     'Value': ValueCW,
                     'Unit': Unit,
                     'StorageResolution': 1
                 },
             ]
     )
      cw_write = {
                 'events' : msn['EVENTS'],
                 'parameter' : 'None',
                 'metricname' : 'session_events',
             }
      save_logs_in_cloudwatch(cw_write)

      count += 1
  return 'Processed ' + str(count) + ' items.'
