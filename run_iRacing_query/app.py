import json
import boto3
import os
import requests
from datetime import timedelta
from dateutil.parser import parse
import pickle

#S3 Details
s3 = boto3.client("s3")
s3_bucket = os.environ['bucket_name']

cookieFileName = os.environ['cookie_file_name']

#SQS Details
sqs = boto3.resource('sqs', region_name='eu-west-2')
apiQueue = sqs.get_queue_by_name(QueueName=os.environ['queue_name'])

#dynamoDB Details
dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

def doesS3FileExist(bucket, file):
    results = s3.list_objects(Bucket=bucket, Prefix=file)
    return 'Contents' in results

def getLoginCredentials(bucket, file):
    if doesS3FileExist(bucket, file):
        return json.loads(s3.get_object(Bucket=bucket, Key=file)["Body"].read())
    return False
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
        return False

def authenticate():
    params = getLoginCredentials(s3_bucket, "credentials.json")
    response = requests.post("https://members-ng.iracing.com/auth", data=params)
    if response.status_code == 200:
        print("Successfully authenticated with iRacing")     
        storeCookie(pickle.dumps(response.cookies), s3_bucket, cookieFileName)
        return response.text
    return False

def getQueryResultList(url, cookie):
    ret = []
    r1 = requests.get(f"{url}", cookies=cookie)
    if r1:
        j = json.loads(r1.text)
        if (j['data']['chunk_info']) and ('base_download_url' in j['data']['chunk_info']) and ('chunk_file_names' in j['data']['chunk_info']):
            for x in j['data']['chunk_info']['chunk_file_names']:
                r2 = requests.get(f"{j['data']['chunk_info']['base_download_url']}{x}")
                #At this stage each iteration of r2.text is a a plain text encoded JSON array
                #Each array is which is terminated after each instance of r2 (which are chunks of raw data)
                #We want to return a single python list from this function
                d = json.loads(r2.text)
                for x in d:
                   ret.append(x)                 
        return ret
    #Re-authentication required - recursively retries the request after authenticating
    elif r1.status_code == 401:
        authenticate()
        cookie = getCookie(s3_bucket, cookieFileName)
        getQueryResultList(url, cookie)
    #Rate limit reached - return False
    elif r1.status_code == 429:
        print ("We are rate limited")
        return False
    #Other error - return False
    else: 
        print (f"iRacing Status Code Error {r1.status_code}")
        print (r1)
        return False

def handleGenerateSessionID(url, cookie):
    #For GenerateSessionID - run the query, isolate the subsession IDs and add them back to the SQS Queue to be run by a subsequent invocation
    #Record the highest finish time and record this to the DB, this is fed into the GenerateSessionID query by the function which generates that
    sessionList = getQueryResultList(url, cookie)
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
            #Disable this to stop thousands of invocations while testing
            #apiQueue.send_message(MessageBody=json.dumps(payload))
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

def handleRetrieveDataQuery(url, cookie):
    #For Retrieve Data we simply run the API query and store the returned value in data queue
    return ""

def lambda_handler(event, context):
    cookie = getCookie(s3_bucket, cookieFileName)

    #Iterate through events, each event should only contain one record but this covers if not
    for record in event['Records']:
        a = record['body']
        a = json.loads(a)
        if a['type'] == 'GenerateSessionID':
            handleGenerateSessionID(a['url'], cookie)
        elif a['type'] == 'RetrieveData':
            print (f"Retrieve Data from {a['url']} requested")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
        }),
    }
