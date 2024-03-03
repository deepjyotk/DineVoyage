import boto3
import json
import logging
import random
from requests_aws4auth import AWS4Auth
import requests
from botocore.exceptions import ClientError, BotoCoreError
import os


# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/533267413906/Q1'
REGION = 'us-east-1'
EMAIL_SOURCE = 'xz4319@nyu.edu'  # Update with your email
DYNAMODB_TABLE_NAME = 'yelp-restaurants-3'
ES_HOST = 'search-restaurants-tf4sd36d6c77zlvuechhryxumi.us-east-1.es.amazonaws.com'
ES_INDEX = 'restaurants'

def receive_messages_from_sqs():
    sqs = boto3.client('sqs', region_name=REGION)
    try:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            AttributeNames=['SentTimestamp'],
            MaxNumberOfMessages=10,
            MessageAttributeNames=['All'],
            VisibilityTimeout=10,
            WaitTimeSeconds=20
        )
        logger.info("Response from SQS: %s", response)
        return response.get('Messages', [])
    except (ClientError, BotoCoreError) as e:
        logger.error("Failed to receive messages from SQS: %s", e)
        return []

def find_restaurants_from_elasticsearch(cuisine):
    credentials = boto3.Session().get_credentials()
    get_env_variable = lambda var_name, default_value=None: os.environ.get(var_name, default_value)
    access_key  = get_env_variable('access_key', 'default_value - access_key')
    secret_access_key  = get_env_variable('secret_access_key', 'default_value - access_key')
    print("ACCess key is ",access_key, " and secret: ", secret_access_key)
    awsauth = AWS4Auth(access_key, secret_access_key, REGION, 'es')
    url = f'https://{ES_HOST}/{ES_INDEX}/_search'

    query = {
        "size": 10,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine
            }
        }
    }

    try:
        response = requests.get(url, auth=awsauth, headers={"Content-Type": "application/json"}, data=json.dumps(query))
        response.raise_for_status()
        restaurants_list = response.json()['hits']['hits']
        restaurants_id_list = [x['_source']['Business ID'] for x in restaurants_list]
        return restaurants_id_list
    except requests.RequestException as e:
        logger.error("Failed to find restaurants from Elasticsearch: %s", e)
        return []

def get_restaurant_details_from_db(restaurant_ids):
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    details = []

    for restaurant_id in restaurant_ids:
        try:
            response = table.get_item(Key={'id': restaurant_id})
            item = response.get('Item')
            if item:
                details.append(item)
            else:
                logger.info("Item with id %s not found.", restaurant_id)
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to get restaurant details from DB for ID %s: %s", restaurant_id, e)
    return details

def create_message(restaurant_details, message_attributes):
    no_of_people = message_attributes['NumberOfPeople']['StringValue']
    time = message_attributes['Time']['StringValue']
    cuisine = message_attributes['Cuisine']['StringValue']
    
    suggestions = [
        f"{i + 1}. {detail['name']}, located at {', '.join(detail['address'])}" for i, detail in enumerate(restaurant_details)
    ]
    suggestions_text = ' '.join(suggestions)
    return f"""Here
    are my {cuisine} restaurant suggestions for {no_of_people} people, at {time}: {suggestions_text}. Enjoy your meal!"""


def send_email_ses(cuisine, message, email):
    
    SES_EMAIL_SUBJECT = "Your Personalized Culinary Journey Awaits!"
    ses_client = boto3.client('ses', region_name=REGION)
    body_of_email = f"""<html>
                    <body>
                    <p>Warm greetings from the team at DineVoyage! We hope this message finds you in great spirits and hungry for new adventures. As your
                    dedicated culinary compass, we're thrilled to embark on a flavorful journey with you, guiding you
                    to the finest dining destinations that cater precisely to your tastes! </p>
                    <br/>
                    <p>{message}</p>
                    <br/>
                    <br/>
                     <p>Best regards,<br>Deepjyot<br>Owner at DineVoyage<br>+1 4433809221</p>
                    </body></html>"""
    try:
        response = ses_client.send_email(
            Source=EMAIL_SOURCE,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': SES_EMAIL_SUBJECT, 'Charset': 'UTF-8'},
                'Body': {
                    'Html': {'Data': f"{body_of_email}", 'Charset': 'UTF-8'}
                }
            }
        )
        logger.info("Email sent! Message ID: %s", response['MessageId'])
        insertInAlreadyCustomer(email , cuisine=cuisine)
    except (ClientError, BotoCoreError) as e:
        logger.error("Failed to send email via SES: %s", e)


def insertInAlreadyCustomer( email , cuisine):
    
    client = boto3.resource(service_name='dynamodb',
                            #   aws_access_key_id="",
                            #   aws_secret_access_key="",
                            region_name="us-east-1",
                            )

    table = client.Table('already-customer')
    try:
        response = table.get_item(Key={'email': email})
        
        # If email exists, update the message
        if 'Item' in response:
            response = table.update_item(
                Key={'email': email},
                UpdateExpression='SET cuisine = :msg',
                ExpressionAttributeValues={':msg': cuisine},
                ReturnValues='UPDATED_NEW'
            )
        else:
            response = table.put_item(Item={'email': email, 'cuisine': cuisine})
        
        print("response from already-customer table : ", response)
        
        return response
    except Exception as e:
        return {"error in saving in insertInAlreadyCustomer": str(e)}
   
    
    
def delete_message_from_sqs(receipt_handle):
    sqs = boto3.client('sqs', region_name=REGION)
    try:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        logger.info("Successfully deleted message with handle: %s", receipt_handle)
    except (ClientError, BotoCoreError) as e:
        logger.error("Failed to delete message : %s. Error: %s", receipt_handle, e)


def checkAlreadyCustomer(receipt_handle):
    pass
def lambda_handler(event, context):
    messages = receive_messages_from_sqs()
    
    for message in messages:
        message_attributes = message['MessageAttributes']
        email = message_attributes['Email']['StringValue']
        cuisine = message_attributes['Cuisine']['StringValue']
        restaurant_ids = find_restaurants_from_elasticsearch(cuisine)
        if restaurant_ids:
            selected_ids = random.sample(restaurant_ids, min(len(restaurant_ids), 3))
            restaurant_details = get_restaurant_details_from_db(selected_ids)
            if restaurant_details:
                msg_to_send = create_message(restaurant_details, message_attributes)
                email = message_attributes['Email']['StringValue']
                send_email_ses(cuisine,msg_to_send ,email)
                delete_message_from_sqs(message['ReceiptHandle'])