#!/usr/bin/env python3
import sys
import json


'''
Initialize the count variables to be displayed on the master shell under the title/group MapperReport.
The following line of code increases the count of 'counterName' in group 'groupName' by x:
    sys.stderr.write("reporter:counter:groupName,counterName,x")
'''

'''
transactionsRecordCount:
    The variable stores the total transaction records read.
'''
sys.stderr.write("reporter:counter:Mapper Report,transactionsRecordCount,0\n")

'''
ipMetadataRecordCount:
    The variable signifies the total Ip-Metadata records read.
'''
sys.stderr.write("reporter:counter:Mapper Report,ipMetadataRecordCount,0\n")

'''
transactionsRecordCountWithStateComplete:
    The variable stores the total transaction records read with state as COMPLETE
'''
sys.stderr.write("reporter:counter:Mapper Report,transactionsRecordCountWithStateComplete,0\n")


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
    isLineOutputedToReducer = False

    #record belongs to the Tranactions table
    if dynamoDBTableToPrimaryKeyMap["Transactions"] in dynamoDBJson:
        sys.stderr.write("reporter:counter:Mapper Report,transactionsRecordCount,1\n")
        primarykeyValue = dynamoDBJson[dynamoDBTableToPrimaryKeyMap["Transactions"]]["s"]
        if dynamoDBJson["state"]["s"] == "COMPLETE":
            sys.stderr.write("reporter:counter:Mapper Report,transactionsRecordCountWithStateComplete,1\n")
            isLineOutputedToReducer = True
   
    #record belongs to Ip-Metadata table
    elif dynamoDBTableToPrimaryKeyMap["Ip-Metadata"] in dynamoDBJson:
        sys.stderr.write("reporter:counter:Mapper Report,ipMetadataRecordCount,1\n")
        primarykeyValue = dynamoDBJson[dynamoDBTableToPrimaryKeyMap["Ip-Metadata"]]["s"]
        isLineOutputedToReducer = True
    
    return primarykeyValue, isLineOutputedToReducer

#Iterate over every line in the input files in s3. Specify the path to input files in the command(available in Command file)
for dynamoDBJson in sys.stdin:
    #Get the primary key value
    primarykeyValue, isLineOutputedToReducer = extractPrimaryKeyValueAndReducerOutputFlag(json.loads(dynamoDBJson))
    #Check if key exists
    if primarykeyValue is not None and isLineOutputedToReducer is True:
        print("{0}\t{1}".format(primarykeyValue, dynamoDBJson), end = '')
