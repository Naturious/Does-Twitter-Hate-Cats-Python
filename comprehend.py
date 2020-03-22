import boto3 # AWS Official Python SDK
import sys # For using command line parameters

## Test script for the AWS Comprehend API using the SDK
# This gets a text from the command line arguments,
# sends it to AWAS Comprehend and print out the response
client = boto3.client('comprehend')

try:
	response = client.detect_sentiment(
	    LanguageCode='en',
	    Text=sys.argv[1],
	)
	print(response)
except Exception as e:
	print(e)
