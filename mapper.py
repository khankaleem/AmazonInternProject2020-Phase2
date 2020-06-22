#!/usr/bin/env python3
import sys
import json

'''
The method extracts a key and identifer from a DynamoDB Json which is read from s3
Input:
    dynamoDBJson:  Json representing a DynamoDB item
Output:
    primarykeyValue: The Primary key value of the DynamoDB Json
    identifier: The identifier is TenantIdTransactionId or RequestId according 
                to the table to which the json belongs
'''
def extractKeyAndIdentifier(dynamoDBJson):

    #initialize primary key value and identifier
    primarykeyValue, identifier = None, None
    
    #record belongs to the Tranactions table
    if "TenantIdTransactionId" in dynamoDBJson:
        primarykeyValue = dynamoDBJson["TenantIdTransactionId"]["s"]
        identifier = "TenantIdTransactionId"
    #record belongs to Ip-Metadata table
    elif "RequestId" in dynamoDBJson:
        primarykeyValue = dynamoDBJson["RequestId"]["s"]
        identifier = "RequestId"

    return primarykeyValue, identifier

#Iterate over every line in the input files
for dynamoDBJson in sys.stdin:
    #Get the primary key value and identifier
    primarykeyValue, identifier = extractKeyAndIdentifier(json.loads(dynamoDBJson))
    #Check if key exists
    if primarykeyValue is not None:
        print("{0}\t{1}".format(primarykeyValue, identifier + '#' + dynamoDBJson), end = '')