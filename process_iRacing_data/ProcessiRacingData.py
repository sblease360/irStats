import json
import boto3
import os
from datetime import date

dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
table = dynamodb.Table(os.environ['table_name'])

def getTrackName(data):
    config = ""
    if data['track']['config_name']:
        config = f" - {data['track']['config_name']}"
    track = f"{data['track']['track_name']}{config}"
    return track

def getCarClassList(data):
    classList = []
    for x in data['session_results']:
        if x['simsession_number'] == 0:
            #The above ensures we only consider race sessions
            #Not important here, but limits the amount of data to iterate through
            for y in x['results']:
                if y['car_class_name'] not in classList:
                    classList.append(y['car_class_name'])
    return classList

def retrieveExistingData(carClass, track):
    #Searches for a particular track and carclass combination and returns the JSON data associated if it exists
    #Returns a blank object if no record is found
    record = table.get_item(
        Key={
            'CarClass'  : carClass,
            'TrackName' : track
        }
    )
    if 'Item' in record: 
        print (record['Item'])
        return record['Item']
    return {}

def generateClassSpecificData(carClass, data):
    #Returns the required payload to store or add to the DB for a specific car class
    classData = {}

    classData['series'] = data['series_name']
    #This year may need revision as 2023_1 could possible start at the end of 2022
    classData['season'] = f"{date.today().year}_{data['season_quarter']}"
    classData['week'] = data['race_week_num']
    classData['temp'] = (float(data['weather']['temp_value']) - 32) * 0.5556

    bestQLap = 99999999
    bestRLap = 99999999

    #Iterate through the results data and store best and average laps for qualifying and best lap from the race
    for x in data['session_results']:
        if x['simsession_name'] == 'QUALIFY':
            for y in x['results']:
                if y['car_class_name'] == carClass:
                    userQLap = y['best_qual_lap_time']
                    if userQLap > 0:
                        if userQLap < bestQLap:
                            bestQLap = userQLap
                            classData['pole_time'] = y['best_lap_time'] / 10000
                            classData['pole_ir'] = y['oldi_rating'] if  y['oldi_rating'] > 0 else ''   
                            classData['pole_driver'] = y['display_name']      
                            classData['pole_car'] = y['car_name']        
        elif x['simsession_name'] == 'RACE':
            for y in x['results']:
                if y['car_class_name'] == carClass:
                    userRLap = y['best_lap_time']
                    if userRLap > 0:
                        if userRLap < bestRLap:
                            bestRLap = userRLap 
                            classData['fastest_lap'] = userRLap / 10000
                            classData['fastest_lap_ir'] = y['oldi_rating'] if  y['oldi_rating'] > 0 else ''
                            classData['fastest_lap_driver'] = y['display_name']   
                            classData['fastest_lap_car'] = y['car_name']       
    return classData

def runDBUpdate(carClass, track, payload):
    update_expression = 'SET {}'.format(','.join(f'#{k}=:{k}' for k in payload))
    expression_attribute_values = {f':{k}': v for k, v in payload.items()}
    expression_attribute_names = {f'#{k}': k for k in payload}

    response = table.update_item(
        Key={
            'CarClass'  : carClass,
            'TrackName' : track
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names,
        ReturnValues='UPDATED_NEW',
    )

def addNewDBEntry(carClass, track, payload):
    #TODO: Write this
    return None

def generateDBPayload(existingData, newData):
    #If there is no existing data, the new data is the payload
    if not existingData:
        return newData
    else:
        #Check if any of the new data needs to replace the existing values
        #This is only required if the pole or fastest lap are faster (lower vals) than the existing
        i = existingData['Item']
        if i['fastest_lap'] > newData['fastest_lap'] and i['pole_time'] > newData['pole_time']:
            #both pole and fastest lap in the new data are faster than the existing data, new data can be written wholesale
            return newData
        elif i['fastest_lap'] > newData['fastest_lap']:
            #Fastest lap is better, but not pole lap, update fastest lap values
            #TODO: The logic here
            return "DUMMY"
        elif i['pole_time'] > newData['pole_time']: 
            #Pole lap is better, but not fastest lap, update pole lap values
            #TODO: As above - logic here
            return "DUMMY"
        else: 
            #Neither lap is better, return existingData as no changes are required
            return existingData
    return None

def lambda_handler(event, context):

    for record in event['Records']:
        data = record['body']
        #Identify car classes and track involved    
        track = getTrackName(data)
        classList = getCarClassList(data)

        for carClass in classList:
            #Generate the specific data for this carClass
            classData = generateClassSpecificData(carClass, data)
            existingData = retrieveExistingData(carClass, track)
            payload = generateDBPayload(existingData, classData)
            if existingData and not payload == existingData:
                runDBUpdate(carClass, track, payload)
            elif not existingData:
                addNewDBEntry(carClass, track, payload)
            #if existingData does exist and payload == existingData then no update is required. 
            
            

    return None
