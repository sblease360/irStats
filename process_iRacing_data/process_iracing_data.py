"""This module handles the data from an SQS queue and records the neccassary in a DynamoDB Table"""

import os
from decimal import Decimal
import json
import boto3

dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

def get_track_name(data):
    """returns the name of the track, complete with the config name if existing from the raw data"""
    config = ""
    if isinstance(data['track'], str):
        return data['track']
    elif 'config_name' in data['track'] and data['track']['config_name']:
        config = f" - {data['track']['config_name']}"
    track = f"{data['track']['track_name']}{config}"
    return track

def get_car_class_list(data):
    """returns a list of the classes in the data"""
    class_list = []
    for i in data['session_results']:
        if i['simsession_number'] == 0:
            #The above ensures we only consider race sessions
            #Not important here, but limits the amount of data to iterate through
            for j in i['results']:
                if j['car_class_name'] not in class_list:
                    class_list.append(j['car_class_name'])
    return class_list

def retrieve_existing_data(car_class, track):
    """Searches the db for a particular track and carclass combination
    returns the JSON data associated if it exists
    Returns a blank object if no record is found"""
    record = table.get_item(
        Key={
            'CarClass'  : car_class,
            'TrackName' : track
        }
    )
    if 'Item' in record:
        return record['Item']
    return {}

def generate_class_specific_data(car_class, data):
    """
    Returns the required payload to store or add to the DB for a specifed car class

    Args:
        car_class (string)  : The specific class which is being considered
    """

    class_data = {
        "pole_time": None,
        "fastest_lap": None,
    }

    temp = (float(data['weather']['temp_value']) - 32) * 0.5556

    best_q_lap = 99999999
    best_r_lap = 99999999

    #Iterate through the results data
    #store best and average laps for qualifying and best lap from the race
    for i in data['session_results']:
        if i['simsession_name'] == 'QUALIFY':
            for j in i['results']:
                if j['car_class_name'] == car_class:
                    user_q_lap = j['best_qual_lap_time']
                    if not user_q_lap:
                        user_q_lap = j['best_lap_time']
                    if user_q_lap > 0:
                        if user_q_lap < best_q_lap:
                            best_q_lap = user_q_lap
                            class_data['pole_time'] = j['best_lap_time'] / 10000
                            class_data['pole_ir'] = j['oldi_rating'] if j['oldi_rating'] > 0 else ''
                            class_data['pole_driver'] = j['display_name']
                            class_data['pole_car'] = j['car_name']
                            class_data['pole_temp'] = temp
        elif i['simsession_name'] == 'RACE':
            for j in i['results']:
                if j['car_class_name'] == car_class:
                    user_r_lap = j['best_lap_time']
                    if user_r_lap > 0:
                        if user_r_lap < best_r_lap:
                            best_r_lap = user_r_lap
                            class_data['fastest_lap'] = user_r_lap / 10000
                            class_data['fastest_lap_ir'] = j['oldi_rating'] if  j['oldi_rating'] > 0 else ''
                            class_data['fastest_lap_driver'] = j['display_name']
                            class_data['fastest_lap_car'] = j['car_name']
                            class_data['fastest_lap_temp'] = temp

    if class_data['fastest_lap'] is None:
        print (f"fastest lap not identified for class {car_class} raw data: {data}")

    return class_data

def run_db_update(car_class, track, payload):
    """Update the db table with the payload values for the provided car class and track"""
    #DynamoDB does not support float, use decimal instead
    for key, value in payload.items():
        if isinstance(value, float):
            #the float -> str -> decimal avoids dynamoDB decimal.inexact and decimal.rounded errors
            payload[key] = Decimal(str(value))

    update_expression = f"SET {','.join(f'#{k}=:{k}' for k in payload)}"
    expression_attribute_values = {f':{k}': v for k, v in payload.items()}
    expression_attribute_names = {f'#{k}': k for k in payload}

    table.update_item(
        Key={
            'CarClass'  : car_class,
            'TrackName' : track
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names,
        ReturnValues='UPDATED_NEW',
    )

def add_new_db_entry(car_class, track, payload):
    """
    Enter new data into the db

    Args:
        car_class (string): class the car belongs to
        track (string): track the data pertains to
        payload (dict): the values to be entered in the data, pertaining to the car_class and track
    Returns:
        Nothing
    """
    payload['CarClass'] = car_class
    payload['TrackName'] = track

    #DynamoDB does not support float, use decimal instead
    for key, value in payload.items():
        if isinstance(value, float):
            #the float -> str -> decimal avoids dynamoDB decimal.inexact and decimal.rounded errors
            payload[key] = Decimal(str(value))

    table.put_item(
        Item=payload
    )
    return None

def generate_db_payload(current, new):
    """
    Compare the existing data with the new data

    Args:
        current (dict)    : the values currently stored in the db
        new (dict)        : the new values derived from the currently processing data
    Returns:
        The payload to be stored in the database
    """

    #If there is no existing data, the new data is the payload
    if not current:
        return new

    #start on the assumption that we will return what is already in the database
    ret_data = current

    #Check if any of the new data needs to replace the existing values
    #This is only required if the pole or fastest lap are faster (lower vals) than the existing
    try:
        if current['fastest_lap'] is None or new['fastest_lap'] < current['fastest_lap']:
            #fastest lap data needs to be taken from the new dataset
            ret_data['fastest_lap'] = new['fastest_lap']
            ret_data['fastest_lap_ir'] = new['fastest_lap_ir']
            ret_data['fastest_lap_driver'] = new['fastest_lap_driver']
            ret_data['fastest_lap_car'] = new['fastest_lap_car']
        if current['pole_time'] is None or new['pole_time'] < current['pole_time']:
            #Pole lap data needs to be taken from the new dataset
            ret_data['pole_time'] = new['pole_time']
            ret_data['pole_time_ir'] = new['pole_ir']
            ret_data['pole_time_driver'] = new['pole_driver']
            ret_data['pole_time_car'] =  new['pole_car']
    except KeyError:
        print (f"Key Error \n Existing: {current} \n New: {new}")
    except TypeError:
        print (f"Type Error comparing values \n Existing: {current} \n New: {new}")

    return ret_data

def lambda_handler(event, context):
    """Main trigger for the module when being run from lambda"""
    #Unused parameters
    del context

    for record in event['Records']:
        data = record['body']
        if not isinstance(data, dict):
            data = json.loads(data)
        #Identify car classes and track involved
        track = get_track_name(data)
        class_list = get_car_class_list(data)

        for car_class in class_list:
            #Generate the specific data for this carClass
            class_data = generate_class_specific_data(car_class, data)
            existing_data = retrieve_existing_data(car_class, track)
            payload = generate_db_payload(existing_data, class_data)
            if existing_data is not None and payload is not None and payload != existing_data:
                run_db_update(car_class, track, payload)
            elif not existing_data:
                add_new_db_entry(car_class, track, payload)
            #if existingData does exist and payload == existingData then no update is required.
