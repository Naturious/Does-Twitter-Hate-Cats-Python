import boto3
import os
import json


comprehend_client = boto3.client('comprehend')

dynamoTableName = os.environ['DYNAMODB_TABLE']

print(f"DynamoDB Tablename: {dynamoTableName}")

dynamoTable = boto3.resource('dynamodb').Table(dynamoTableName)

def lambda_handler(event, context):
    
    # Loop over the DynamoDB Stream records 
    for record in event['Records']:
        if(record['eventName'] == 'INSERT'):

            # Calls the AWS Comprehend API to get the sentiment analysis
            comprehendRes = comprehend_client.detect_sentiment(
                LanguageCode='en',
                Text=record['dynamodb']['NewImage']['tweet']['S'],
            )
            print(f"comprehendRes: {comprehendRes}")

            # Updates the matching row for that sentiment in the result DynamoDB database
            dynamoParams = {
                'Key': {
                    "sentiment": comprehendRes['Sentiment']
                },
                'UpdateExpression': "ADD tweets :val",
                'ConditionExpression': "attribute_not_exists(sentiment) OR sentiment = :sentiment",
                'ExpressionAttributeValues': {
                    ":val": 1,
                    ":sentiment": comprehendRes['Sentiment']
                },
                'ReturnValues': "UPDATED_NEW"
            }
            dynamoRes = dynamoTable.update_item(**dynamoParams)

            print(f"DynamoDB put response: {dynamoRes}")

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }