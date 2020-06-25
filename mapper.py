#!/usr/bin/env python3
import sys
import json

'''
The method extracts the primary key value from a DynamoDB Json which is read from s3.
Also extracts the reducer output flag of the json.  This reducer output flag specifies
if the current line should be outputed to the reducer
Input:
    dynamoDBJson:  Json representing a DynamoDB item
Output:
    primarykeyValue: The Primary key value of the DynamoDB Json
    isLineOutputedToReducer: The reducer output flag is true for all ip-metadata records and true for those transactions records having state as COMPLETE
'''
def extractPrimaryKeyValueAndReducerOutputFlag(dynamoDBJson):

    #Map containg key as Table name and value as primary key field
    dynamoDBTableToPrimaryKeyMap = {"Transactions" : "TenantIdTransactionId", "Ip-Metadata" : "RequestId"}

    #initialize primaryKeyValue and reducer output flag
    primarykeyValue = None
    isLineOutputedToReducer = True

    #record belongs to the Tranactions table
    if dynamoDBTableToPrimaryKeyMap["Transactions"] in dynamoDBJson:
        primarykeyValue = dynamoDBJson[dynamoDBTableToPrimaryKeyMap["Transactions"]]["s"]
        if "state" in dynamoDBJson and dynamoDBJson["state"]["s"] != "COMPLETE":
            isLineOutputedToReducer = False
    #record belongs to Ip-Metadata table
    elif dynamoDBTableToPrimaryKeyMap["Ip-Metadata"] in dynamoDBJson:
        primarykeyValue = dynamoDBJson[dynamoDBTableToPrimaryKeyMap["Ip-Metadata"]]["s"]

    return primarykeyValue, isLineOutputedToReducer

#Iterate over every line in the input files in s3. Specify the path to input files in the command(available in Command file)
for dynamoDBJson in sys.stdin:
    #Get the primary key value
    primarykeyValue, isLineOutputedToReducer = extractPrimaryKeyValueAndReducerOutputFlag(json.loads(dynamoDBJson))
    #Check if key exists
    if primarykeyValue is not None and isLineOutputedToReducer is True:
        print("{0}\t{1}".format(primarykeyValue, dynamoDBJson), end = '')
