import boto3
import os
import json

s3client = boto3.client('s3')
dynamoclient = boto3.client('dynamodb')

def lambda_handler(event, context):

    # This will pull data from S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f"Bucket: {bucket}, Key: {key}")

    s3response = s3client.get_object(Bucket=bucket,Key=key)

    # Decode data and split it into individual json records
    dynamoData = s3response['Body'].read().decode('utf-8').split('|')

    dynamoData.pop() # Remove last entry in the array because of trailing pipe character

    dynamoTableName = os.environ['DYNAMODB_TABLE']

    print(f"DynamoDB Tablename: {dynamoTableName}")

    # This is will push data into dynamo

    for row in dynamoData:
        item = json.loads(row) # Get object representation out of json string
        item['id'] = {'N':str(item['id'])} 
        item['timestamp'] = {'S':item['timestamp']}
        item['tweet'] = {'S':item['tweet']}
        
        print(f"item:{item}")
        dynamoRes = dynamoclient.put_item(TableName = dynamoTableName, Item = item)
        print(f"DynamoDB put response: {dynamoRes}")

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }