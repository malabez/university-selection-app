from __future__ import print_function

import json
import requests
import yaml
import boto3
import uuid
import logging
import datetime

log = logging.getLogger()
log.setLevel(logging.INFO)

ka = boto3.client('kinesisanalytics')
s3 = boto3.client('s3')
cf = boto3.client('cloudformation')
cw = boto3.client('cloudwatch')

def sendResponse(event, responseStatus, resourceId, reason, uuId):
    responseBody = {'Status': responseStatus,
                    'PhysicalResourceId': resourceId,
                    'StackId': event['StackId'],
                    'RequestId': event['RequestId'],
                    'LogicalResourceId': event['LogicalResourceId'],
                    'Data': {
                        'UUID': str(uuId),
                        'AppName': resourceId
                    }}
    if responseStatus == "FAILED":
        responseBody['Reason'] = reason
    log.info('RESPONSE BODY:n' + json.dumps(responseBody))
    try:
        requests.put(event['ResponseURL'], data=json.dumps(responseBody))
        return
    except Exception as e:
        log.error(e)
        raise

def deleteApplication(event, context):
    applist = ka.list_applications()
    for i in applist['ApplicationSummaries']:
        if i['ApplicationName'] == event['PhysicalResourceId']:
            try:
                log.info('Attempting to delete Kinesis Analytics application "' + event['PhysicalResourceId'] + '"')
                appDesc = ka.describe_application(ApplicationName=event['PhysicalResourceId'])
                createTs = appDesc['ApplicationDetail']['CreateTimestamp']
                deleteResponse = ka.delete_application(ApplicationName=event['PhysicalResourceId'],CreateTimestamp=createTs)
                # If API call does not return 200, script fails
                if deleteResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
                    responseStatus = 'SUCCESS'
                    log.info('Application successfully deleted')
                    sendResponse(event, responseStatus, event['PhysicalResourceId'], None, None)
                else:
                    responseStatus = "FAILED"
                    log.error('Delete application API call failed to return 200')
                    reason = context.log_stream_name
                    sendResponse(event, responseStatus, event['PhysicalResourceId'], reason, None)
            except Exception as e:
                log.error(e)
                responseStatus = 'FAILED'
                reason = context.log_stream_name
                sendResponse(event, responseStatus, event['PhysicalResourceId'], reason, None)
        else:
            log.info('Application not detected, nothing to delete')
            responseStatus = 'SUCCESS'
            sendResponse(event, responseStatus, event['PhysicalResourceId'], None, None)

    # Send notification to AWS if customer wants to provide anonymous metrics
    if event['ResourceProperties']['AnonymousData'] == "Yes":
        try:
            log.info('Sending delete notification to AWS')
            stackDesc = cf.describe_stacks(StackName= event['StackId'])
            outputList = stackDesc['Stacks'][0]['Outputs']
            for o in outputList:
                if o['OutputKey'] == 'UUID':
                    uuid=(o['OutputValue'])
                    deleteDict = {"event":"deleteApplication"}
                    notifyAWS(deleteDict, uuid)
        except Exception as e:
            log.error(e)
            pass

def createApplication(event,context):
    log.info('Generating Kinesis Analytics Application Id')
    appId = str(uuid.uuid4()).split("-")
    appId = appId[4]
    KAAppName = event['ResourceProperties']['KAAppName']+appId
    log.info('Attempting to create Kinesis Analytics application "' + KAAppName + '"')
    try:
        FunctionName = event['ResourceProperties']['FunctionName']
        Region = event['ResourceProperties']['Region']
        KArole = event['ResourceProperties']['KARole']
        Destination = event['ResourceProperties']['Destination']
        PersistAggData = event['ResourceProperties']['PersistAggData']
        SourceStream = event['ResourceProperties']['SourceStream']
        print(FunctionName)
        print(Destination)
        print(KArole)
        print(PersistAggData)
        print(SourceStream)

        if 'LambdaDest' in event['ResourceProperties']:
            LambdaDest = event['ResourceProperties']['LambdaDest']
            outputARN = LambdaDest
        if 'RedshiftDest' in event['ResourceProperties']:
            RedshiftDest = event['ResourceProperties']['RedshiftDest']
            outputARN = RedshiftDest
        if 'S3Dest' in event['ResourceProperties']:
            S3Dest = event['ResourceProperties']['S3Dest']
            outputARN = S3Dest
        if 'ElasticsearchDest' in event['ResourceProperties']:
            ElasticsearchDest = event['ResourceProperties']['ElasticsearchDest']
            outputARN = ElasticsearchDest
        if 'StreamDest' in event['ResourceProperties']:
            StreamDest = event['ResourceProperties']['StreamDest']
            outputARN = StreamDest
        if event['ResourceProperties']['AnonymousData'] == "Yes":
            NewId = createUuid()
        else:
            NewId = None

        # If config file is provided, use the custom configuration
        if 'ConfigFile' in event['ResourceProperties']:
            log.info('config file detected, applying custom configuration')
            ConfigFile = event['ResourceProperties']['ConfigFile']

            # Get KAconfig.yaml
            log.info('loading configuration file')
            bucket = ConfigFile.split("/",1)[0]
            key = ConfigFile.split("/",1)[1]
            cfgfile = (s3.get_object(Bucket=bucket,Key=key)['Body'].read())
            kaconfig = yaml.load(cfgfile)
            log.info('configuration file successfully loaded')

            # Parse record format from yaml file
            log.info('parsing record format from configuration file')
            inputrecordformat = kaconfig['format']['InputFormatType']
            if Destination == "Amazon Redshift":
                outputrecordformat = 'CSV'
            elif Destination == "Amazon ElasticSearch Service":
                outputrecordformat = 'JSON'
            elif Destination == "LambdaDest":
                outputrecordformat = 'JSON'
            else:
                outputrecordformat = kaconfig['format']['OutputFormatType']

            # Parse record columns from yaml file
            log.info('parsing record columns from configuration file')
            if kaconfig['columns'] == None:
                log.info('no columns detected, applying a "catcha-all" schema')
                recordcolumns = [{'Name': 'Data', 'SqlType':'VARCHAR(5000)'}]
            else:
                recordcolumns = kaconfig['columns']

            # Parse application code from yaml file
            log.info('parsing application code from configuration file')
            applicationcode = kaconfig['sql_code']

            # Create Kinesis Analytics application mapping parameters based on format in yaml file
            mappingParameters = {}
            if kaconfig['format']['InputFormatType'] == "CSV":
                log.info('creating CSV mapping parameters')
                mappingParameters = {'CSVMappingParameters': {'RecordRowDelimiter':kaconfig['format']['RecordRowDelimiter'], 'RecordColumnDelimiter': kaconfig['format']['RecordColumnDelimiter']}}
            elif kaconfig['format']['InputFormatType'] == "JSON":
                log.info('creating JSON mapping parameters')
                mappingParameters = {'JSONMappingParameters': {'RecordRowPath': kaconfig['format']['RecordRowPath']}}

            # Create Kinesis Analytics output based on the external destination selected by the user in the CloudFormation template
            applicationOutput = []
            if Destination == "Amazon Kinesis Stream":
                outputType = 'KinesisStreamsOutput'
            elif Destination == "LambdaDest":
                outputType = 'LambdaOutput'
            else:
                outputType = 'KinesisFirehoseOutput'
            log.info('adding output configuration for '+ outputType)
            aggregatedOutput = {'Name': 'DESTINATION_SQL_STREAM',outputType: {'ResourceARN': outputARN, 'RoleARN': KArole},"DestinationSchema": {"RecordFormatType":outputrecordformat}}

            applicationOutput.append(aggregatedOutput)

            # Create Kinesis Analytics application code
            log.info('parsing and adding custom SQL code')
            sqlcodestring = kaconfig['sql_code']

        # If no config file was provided, this is the default configuration
        else:
            log.info('no config file detected, applying default configuration')
            if Destination == "Amazon Kinesis Stream":
                outputType = 'KinesisStreamsOutput'
            else:
                outputType = 'KinesisFirehoseOutput'
            inputrecordformat = "CSV"
            outputrecordformat = "CSV"
            recordcolumns = [{'Name': 'Data', 'SqlType':'VARCHAR(5000)'}]
            sqlcodestring = "--Edit the application code below\nCREATE OR REPLACE STREAM \"DESTINATION_SQL_STREAM\" (Data VARCHAR(5000));\nCREATE OR REPLACE PUMP \"STREAM_PUMP\" AS INSERT INTO \"DESTINATION_SQL_STREAM\"\n"
            mappingParameters = {'CSVMappingParameters': {'RecordRowDelimiter':"\\n", 'RecordColumnDelimiter': ","}}
            applicationOutput = [{'Name': 'DESTINATION_SQL_STREAM',outputType: {'ResourceARN': outputARN, 'RoleARN': KArole},"DestinationSchema": {"RecordFormatType":outputrecordformat}}]

        # Use the Kinesis Analytics API to create the application
        log.info('creating application')
        createResponse = ka.create_application(
            ApplicationName=KAAppName,
            Inputs=
            [{'NamePrefix': 'SOURCE_SQL_STREAM',
            'KinesisStreamsInput': {'ResourceARN':SourceStream,'RoleARN':KArole},
            'InputSchema': {
                'RecordFormat': {
                    'RecordFormatType':inputrecordformat, 'MappingParameters': mappingParameters
                },
                'RecordEncoding': 'UTF-8',
                'RecordColumns':recordcolumns
            }
            }],
            Outputs=applicationOutput,
            ApplicationCode=sqlcodestring)

        # If API call does not return 200, script fails
        if createResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
            responseStatus = 'SUCCESS'
            log.info('Application successfully created')
            sendResponse(event, responseStatus, KAAppName, None, NewId)
        else:
            responseStatus = "FAILED"
            log.error('Create application API call failed to return 200')
            errorlogs = 'https://' + Region + '.console.aws.amazon.com/cloudwatch/home?region=' + Region + '#logEventViewer:group=/aws/lambda/' + FunctionName
            reason = 'See details in Amazon CloudWatch: '+ errorlogs
            sendResponse(event, responseStatus, KAAppName, reason, None)
    except Exception as e:
        responseStatus = "FAILED"
        log.error(e)
        errorlogs = 'https://' + Region + '.console.aws.amazon.com/cloudwatch/home?region=' + Region + '#logEventViewer:group=/aws/lambda/' + FunctionName
        reason = 'See details in Amazon CloudWatch: '+ errorlogs
        sendResponse(event, responseStatus, KAAppName, reason, None)

    # Send notification to AWS if customer wants to provide anonymous metrics
    if event['ResourceProperties']['AnonymousData'] == "Yes":
        try:
            configDict = {
            "event":"createApplication", "appConfig": {
                "destination":Destination, "persistAggData":PersistAggData, "format":inputrecordformat
                }
            }
            notifyAWS(configDict, NewId)
        except Exception as e:
            log.error(e)
            pass

def notifyAWS(eventdata, uuid):
    try:
        time_now = datetime.datetime.utcnow().isoformat()
        time_stamp = str(time_now)
        postDict = {}
        postDict['Data'] = eventdata
        postDict['TimeStamp'] = time_stamp
        postDict['Solution'] = 'SO0014'
        postDict['UUID'] = str(uuid)
        url = 'https://metrics.awssolutionsbuilder.com/generic'
        data=json.dumps(postDict)
        headers = {'content-type': 'application/json'}
        req = Request(url, data, headers)
        rsp = urlopen(req)
        content = rsp.read()
        rspcode = rsp.getcode()
        print('Response Code: {}'.format(rspcode))
        print('Response Content: {}'.format(content))
    except Exception as e:
        log.error(e)
        pass

def createUuid():
    try:
        log.info('Generating UUID')
        uniqueID = uuid.uuid4()
        return uniqueID
    except Exception as e:
        log.error(e)
        pass

def lambda_handler(event, context):
    print(event)
    if event['RequestType'] == 'Delete':
        deleteApplication(event, context)
    if event['RequestType'] == 'Create':
        createApplication(event,context)
    # Triggered by CloudWatch rule - only if customer wants to provide anonymous metrics
    if event['RequestType'] == 'SendAnonymousMetrics':
        try:
        app = ka.describe_application(ApplicationName=event['AppName'])
        inputId = app['ApplicationDetail']['InputDescriptions'][0]['InputId']
        InputRecords = cw.get_metric_statistics(
            Namespace='AWS/KinesisAnalytics',
            MetricName='Records',
            Dimensions=[
                {
                    'Name': 'Flow',
                    'Value': 'Input'
                },
                {
                    'Name': 'Application',
                    'Value': event['AppName']
                },
                {
                    'Name': 'Id',
                    'Value': str(inputId)
                }
            ],
            StartTime=datetime.datetime.utcnow()-datetime.timedelta(minutes=15),
            EndTime=datetime.datetime.utcnow(),
            Period=900,
            Statistics=[
                'Sum'
            ]
        )
        InputBytes = cw.get_metric_statistics(
            Namespace='AWS/KinesisAnalytics',
            MetricName='Bytes',
            Dimensions=[
                {
                    'Name': 'Flow',
                    'Value': 'Input'
                },
                {
                    'Name': 'Application',
                    'Value': event['AppName']
                },
                {
                    'Name': 'Id',
                    'Value': str(inputId)
                }
            ],
            StartTime=datetime.datetime.utcnow()-datetime.timedelta(minutes=15),
            EndTime=datetime.datetime.utcnow(),
            Period=900,
            Statistics=[
                'Sum'
            ]
        )
        usageMetrics = {
            "event":"appUsageMetrics",
            "metrics": {
                "InputRecords": InputRecords['Datapoints'][0]['Sum'],
                "InputBytes": InputBytes['Datapoints'][0]['Sum'],
            }
        }
        notifyAWS(usageMetrics, event['uuid'])
    except Exception as e:
        log.error(e)
        pass

