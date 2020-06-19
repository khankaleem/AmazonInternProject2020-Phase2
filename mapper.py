#!/usr/bin/env python3
import sys

#Possible primary keys
primaryKeys = ["TenantIdTransactionId", "RequestId"]

'''
The method extracts a key and identifer from a DynamoDB Json in string format
Input:
    dynamoDBJson: String containing a DynamoDB Json
Output:
    primarykey_value: The Primary key value of the DynamoDB Json
    identifier: The identifier is 0 if the Json belongs to the transactions table,
                and 1 if the Json belongs to the Ip-Metadata table. 
'''
def extractKeyAndIdentifier(dynamoDBJson):

    for identifier in range(2):
        #get primary key
        primary_key = primaryKeys[identifier]
        #check if primary key is present in dynamoDBJson
        if primary_key in dynamoDBJson:
            
            #Find the value of primary key
            index = dynamoDBJson.find(primary_key)
            start = index + len(primary_key + '":{"s":')
            end = start
            while dynamoDBJson[end] != '}':
                end = end + 1
            primarykey_value = dynamoDBJson[start+1:end-1]
            
            #return the value of primary key and the identifier
            return primarykey_value, identifier

    return None, None


#Iterate over every line in the input files
for dynamoDBJson in sys.stdin:
    #Get the key and identifier
    key, identifier = extractKeyAndIdentifier(dynamoDBJson)
    #Check if key exists
    if key is not None:
        #Attach dynamoDBJson to identifier
        identifier_dynamoDBJson = str(identifier) + '^' + dynamoDBJson
        #Print the key and identifier_dynamoDBJson pair
        print("{0}\t{1}".format(key, identifier_dynamoDBJson), end = '')