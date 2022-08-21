import json
import boto3
import os
import requests
from datetime import timedelta
from dateutil.parser import parse
import pickle

def doesS3FileExist(bucket, file):
    results = s3.list_objects(Bucket=bucket, Prefix=file)
    return 'Contents' in results

def getLoginCredentials(bucket, file):
    #TODO: Replace this with AWS Secret Manager
    if doesS3FileExist(bucket, file):
        return json.loads(s3.get_object(Bucket=bucket, Key=file)["Body"].read())
    return {}
    #accessed via dict_name["email"] and dict_name["password"]

def storeCookie(text, bucket, cookieFile):
    s3.put_object(Body=text, Bucket=bucket, Key=cookieFile)

def getCookie(bucket, file):
    if doesS3FileExist(bucket, file):
        return pickle.loads(s3.get_object(Bucket=bucket, Key=file)["Body"].read())
    else:
        a = authenticate()
        if a:
            getCookie(bucket, file)
        return ""

def authenticate():
    params = getLoginCredentials(s3_bucket, "credentials.json")
    response = requests.post("https://members-ng.iracing.com/auth", data=params)
    if response.status_code == 200:
        print("Successfully authenticated with iRacing")     
        storeCookie(pickle.dumps(response.cookies), s3_bucket, cookieFileName)
        return True
    return False

def getQueryText(url, cookie):
    r = requests.get(f"{url}", cookies=cookie)
    if r: 
        return r.text
    elif r.status_code == 401:
        #Authentication error - re-auth and try again
        authenticate()
        getQueryText(url, cookie)
    elif r.status_code == 429:
        #print for logs and return blank
        print ("We are rate limited")
        return ""
    else: 
        #Other error, print for logs and return
        print (f"iRacing Status Code Error {r.status_code}")
        print (r.text)
        return ""

def getSessionIDListQueryResult(url, cookie):
    #Returns a list of results
    #Used for queries that can be expected to return a JSON array
    ret = []
    responseText = getQueryText(url, cookie)
    if responseText:
        i = json.loads(responseText)
        if (i['data']['chunk_info']) and ('base_download_url' in i['data']['chunk_info']) and ('chunk_file_names' in i['data']['chunk_info']):
            for j in i['data']['chunk_info']['chunk_file_names']:
                r2 = requests.get(f"{i['data']['chunk_info']['base_download_url']}{j}")
                #At this stage each iteration of r2.text is a a plain text encoded JSON array
                #Each array is terminated after each instance of r2 (which are chunks of raw data)
                #We want to return a single python list from this function
                data = json.loads(r2.text)
                for k in data:
                   ret.append(k)    
        return ret

def handleGenerateSessionID(url, cookie):
    #For GenerateSessionID - run the query, isolate the subsession IDs and add them back to the SQS Queue to be run by a subsequent invocation
    #Record the highest finish time and record this to the DB, this is fed into the GenerateSessionID query by the function which generates that
    sessionList = getSessionIDListQueryResult(url, cookie)
    if sessionList:
        maxtime = parse("2000-01-01T00:00:00Z")
        for i in sessionList:
            t = parse(i['end_time'])
            if t > maxtime:
                maxtime = t
            newUrl = f"https://members-ng.iracing.com/data/results/get?subsession_id={i['subsession_id']}"
            payload = {
                'type': 'RetrieveData',
                'url' : newUrl
            }
            #Disable this line to stop thousands of invocations while testing
            apiQueue.send_message(MessageBody=json.dumps(payload))
        table.update_item(
            Key={
            'Parameter': 'finish_range_begin'
            },
            ExpressionAttributeNames={
                '#value': 'Value'
            },
            ExpressionAttributeValues={
                ':nv': maxtime.strftime('%Y-%m-%dT%H:%MZ')
            },
            UpdateExpression='SET #value = :nv',
            ReturnValues='UPDATED_NEW'
        )
        return True
    return False

def handleRetrieveDataQuery(url, cookie):
    #For Retrieve Data we simply run the API query to get a link to the data
    #The data is then added to the SQS queue as JSON for further processing
    payload = {}
    i = getQueryText(url, cookie)
    try:
        payload = json.loads(i)
        if 'link' in payload:
            j = getQueryText(payload['link'] ,cookie)
            payload = json.loads(j)
            dataQueue.send_message(MessageBody=json.dumps(payload))
            return True
    except Exception as e:
        print (f"{type(e)}: {str(e)}")
        return False

#S3 Details
s3 = boto3.client("s3")
s3_bucket = os.environ['bucket_name']

cookieFileName = os.environ['cookie_file_name']

#SQS Details
sqs = boto3.resource('sqs', region_name='eu-west-2')
apiQueue = sqs.get_queue_by_name(QueueName=os.environ['api_queue_name'])
dataQueue = sqs.get_queue_by_name(QueueName=os.environ['data_queue_name'])

#dynamoDB Details
dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

cookie = getCookie(s3_bucket, cookieFileName)

def lambda_handler(event, context):
        #Iterate through events, each event should only contain one record but this covers if not
    for record in event['Records']:
        a = record['body']
        a = json.loads(a)
        if a['type'] == 'GenerateSessionID':
            handleGenerateSessionID(a['url'], cookie)
        elif a['type'] == 'RetrieveData':
            handleRetrieveDataQuery(a['url'], cookie)

    return None