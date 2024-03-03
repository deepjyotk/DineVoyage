import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)



def sendMsg(slots):
    sqs = boto3.client('sqs')
    queue_url =  'https://sqs.us-east-1.amazonaws.com/533267413906/Q1'
    print("Trigger sendMsg: ", slots)
    print("email is: ",slots["Email"], slots['NumberOfPeople'], slots['Location'], slots['DiningTime'], slots['Cuisine'])
    print("Queue url: ", queue_url)
    Attributes={
        'NumberOfPeople': {
            'DataType': 'String',
            'StringValue': slots["NumberOfPeople"]
        },
        'Location': {
            'DataType': 'String',
            'StringValue': slots["Location"]
        },
        'Time': {
            'DataType': 'String',
            'StringValue': slots["DiningTime"]
        },
        'Email' : {
            'DataType': 'String',
            'StringValue': slots["Email"]
        },
        'Cuisine': {
            'DataType': 'String',
            'StringValue': slots["Cuisine"]
        }
    }
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes=Attributes,
        MessageBody=('Testing queue')
        )
    print('response is: ' + response['MessageId'])

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def safe_int(n):
    if n is not None:
        return int(n)
    return n


def try_ex(func):
    try:
        return func()
    except KeyError:
        return None


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_cuisine(cuisine):
    cuisines = ['indian',  'chinese']
    return cuisine.lower() in cuisines

def isvalid_numberofpeople(numPeople):
    numPeople = safe_int(numPeople)
    if numPeople > 10 or numPeople < 0:
        return False
        
def isvalid_date(diningdate):
    if datetime.datetime.strptime(diningdate, '%Y-%m-%d').date() <= datetime.date.today():
        return False

def isvalid_time(diningdate, diningtime):
    if datetime.datetime.strptime(diningdate, '%Y-%m-%d').date() == datetime.date.today():
        if datetime.datetime.strptime(diningtime, '%H:%M').time() <= datetime.datetime.now().time():
            return False

def validate_dining_suggestion(cuisine, numPeople, diningtime):
    if cuisine is not None:
        if not isvalid_cuisine(cuisine):
            return build_validation_result(False, 'Cuisine', 'Cuisine not available. Please try another.')
    
    if numPeople is not None:
        if not isvalid_numberofpeople(numPeople):
            return build_validation_result(False, 'NumberOfPeople', 'Maximum 10 people allowed. Try again')
            
            

    return build_validation_result(True, None, None)





def greetings(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi, how can I help?'}
        }
    }

def thank_you(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Welcome!'}
        }
    }

def checkAlreadyCustomer(email):
    logger.info("calling checkAlreadyCustomer", email)
    client = boto3.resource(service_name='dynamodb',region_name="us-east-1")

    table = client.Table('already-customer')
    try:
        response = table.get_item(Key={'email': email})
        logger.info("checkAlreadyCustomer: ",response)
        
        # If email exists, update the message
        if 'Item' in response:
           return True
        else:
            return False
    except Exception as e:
        logger.error("checkAlreadyCustomer error is: ",e)
        return False
    
def dining_suggestions(intent_request):
    slots = intent_request['currentIntent']['slots']
    cuisine = slots["Cuisine"]
    numPeople = slots["NumberOfPeople"]
    diningtime = slots["DiningTime"]
    location = slots["Location"]
    Email = slots["Email"]
   
    
    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        validation_result = validate_dining_suggestion(cuisine, numPeople, diningtime)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
    
        if intent_request[
                'sessionAttributes'] is not None:
                output_session_attributes = intent_request['sessionAttributes']
        else:
            output_session_attributes = {}
    
        return delegate(output_session_attributes, intent_request['currentIntent']['slots'])
    
    
    # Email = str(Email)
    # old_new = str(old_new)
    
    # print("Email is: ",Email)
    # print("old_new is: ",old_new)
    # if checkAlreadyCustomer(Email) and (old_new=="Yes" or old_new=="Old") :
    #     old_cuisine = getOldCuisine(Email)
    #     print("OLD cuisine: ", old_cuisine)
    #     print("current cuisine: ", cuisine)
    #     slots['Cuisine'] = old_cuisine
        
            
    # after fulfilment calling sqs
    sendMsg(slots)
    # print("Slo")
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! You will recieve suggestion shortly'})
    
def getOldCuisine(email):
    try:
        client = boto3.resource(service_name='dynamodb',
                            #   aws_access_key_id="",
                            #   aws_secret_access_key="",
                            region_name="us-east-1",
                            )
        table = client.Table('already-customer')
      
        
        response = table.get_item(Key={'email': email})
        if 'Item' in response:
            return response['Item'].get('cuisine')
    except Exception as e:
        logger.error("getOldCuisine error is: ",e)
        return None

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    #logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    print("intent_request is : ", intent_request)
    intent_name = intent_request['currentIntent']['name']
    # print
    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'GreetIntent':
        return greetings(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    print("event in handler: ", event)
    return dispatch(event)