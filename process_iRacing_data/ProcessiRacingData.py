import json
import boto3
import os

dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

def lambda_handler(event, context):

    for record in event['Records']:
        a = record['body']
        a = json.loads(a)

        print (a)

    return None
