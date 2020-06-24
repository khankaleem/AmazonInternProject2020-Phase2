#!/usr/bin/env python3
import sys
import json

'''
The method extracts the primary key value from a DynamoDB Json which is read from s3
Input:
    dynamoDBJson:  Json representing a DynamoDB item
Output:
    primarykeyValue: The Primary key value of the DynamoDB Json
'''
def extractPrimaryKeyValue(dynamoDBJson):

    #Map containg key as Table name and value as primary key field
    MapTableToPrimaryKey = {"Transactions" : "TenantIdTransactionId", "Ip-Metadata" : "RequestId"}

    #initialize primary key value and identifier
    primarykeyValue = None

    #record belongs to the Tranactions table
    if MapTableToPrimaryKey["Transactions"] in dynamoDBJson:
        primarykeyValue = dynamoDBJson[MapTableToPrimaryKey["Transactions"]]["s"]
    #record belongs to Ip-Metadata table
    elif MapTableToPrimaryKey["Ip-Metadata"] in dynamoDBJson:
        primarykeyValue = dynamoDBJson[MapTableToPrimaryKey["Ip-Metadata"]]["s"]
    return primarykeyValue

#Iterate over every line in the input files in s3. Specify the path to input files in the command available in Command fil.e
for dynamoDBJson in sys.stdin:
    #Get the primary key value
    primarykeyValue = extractPrimaryKeyValue(json.loads(dynamoDBJson))
    #Check if key exists
    if primarykeyValue is not None:
        print("{0}\t{1}".format(primarykeyValue, dynamoDBJson), end = '')