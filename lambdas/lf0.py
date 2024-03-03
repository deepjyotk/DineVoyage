import json
import boto3
from datetime import datetime, timedelta, timezone

def get_est_time():
    return timezone(timedelta(hours=-4))

def lambda_handler(event, context):
    client = boto3.client('lex-runtime', region_name='us-east-1')
    
    try:
        body = json.loads(event['body'])
        messageR = body['messages'][0]['unstructured']['text']
        
        lex_response = client.post_text(
            botName='RestaurantFinderChatBot',
            botAlias='chatty',
            userId='300',
            inputText=messageR)
        
        message = lex_response['message']
        ctime = datetime.now(get_est_time())
        date_str = f"{ctime.hour}:{ctime.minute}"
        
        response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type, Origin, X-Auth-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps({
                "messages": [
                    {
                        "type": "unstructured",
                        "unstructured": {
                            "id": "1",
                            "text": message,
                            "timestamp": date_str
                        }
                    }
                ]
            })
        }
    except KeyError as e:
        response = {
            'statusCode': 400,
            'body': json.dumps({'error': f"Missing key in request: {e}"})
        }
    except Exception as e:
        response = {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        
    return response