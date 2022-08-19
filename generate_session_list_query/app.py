import json
import boto3
import os
from dateutil.parser import parse
from datetime import timedelta

### Reads the most recently used time from the DynamoDB table and constructs a Query to get all race events since this time
### Query string is added to an SQS Queue to be processed by another Lambda function

dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

sqs = boto3.resource('sqs', region_name='eu-west-2')
queue = sqs.get_queue_by_name(QueueName=os.environ['queue_name'])

def get_prev_time_from_dynamoDB():
    #Pull the finish time from the specific table, if it exists return it
    #If it doesn't exist, return the environment variable containing the default value
    response = table.get_item(
        Key={
           'Parameter': 'finish_range_begin' 
        }
    )
    #If the value doesn't exist, the response does not contain item
    if 'Item' in response:
        return response['Item']['Value']
    return os.environ['default_time']

def get_start_time_from_finish_time(finish_time):
    #Return a string formatted time 1.5 days before the start date
    #1.5 days ensures that even 24H race sessions are picked up
    x = parse(finish_time)
    y = x - timedelta(days=1.5)
    return y.strftime('%Y-%m-%dT%H:%MZ')

def lambda_handler(event, context):

    official_only = os.environ['official_only']
    event_types = os.environ['event_types']
    category_ids = os.environ['category_ids']
    finish_range_begin = get_prev_time_from_dynamoDB()
    start_range_begin = get_start_time_from_finish_time(finish_range_begin)

    url = f"https://members-ng.iracing.com/data/results/search_series?official_only={official_only}&event_types={event_types}&category_ids={category_ids}&finish_range_begin={finish_range_begin}&start_range_begin={start_range_begin}"

    print (url)
    
    payload = {
        'type': 'GenerateSessionID',
        'url' : url
    }

    queue.send_message(MessageBody=json.dumps(payload))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
        }),
    }
