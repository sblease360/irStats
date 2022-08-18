import json
import boto3
import os
import requests
import pickle

s3 = boto3.client("s3")
s3_bucket = os.environ['bucket_name']

cookieFileName = os.environ['cookie_file_name']

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
        #print (r1.text)
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
        return False

def lambda_handler(event, context):
    cookie = getCookie(s3_bucket, cookieFileName)

    for record in event['Records']:
        payload = record['body']
        url = payload['url']
        if payload['type'] == 'GenerateSessionID':
            sessionList = getQueryResultList(url, cookie)
            print (sessionList)
            
        

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
        }),
    }
