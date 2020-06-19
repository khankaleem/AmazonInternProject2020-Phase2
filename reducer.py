#!/usr/bin/env python3
import sys
import json

'''
The methods applies reverse mapping to ip-metadata column names.
Input: 
    dynamoDBJson: Ip-Metadata record in the form of string
Output:
    dynamoDBJson with renamed columns
'''
def changeColumnNames(dynamoDBJson):
    '''
    Build the reverse mapping.
    Change the mapping as per the usecase
    '''
    ipMetadata_Transactions_Mapping = {}
    ipMetadata_Transactions_Mapping["documentExchangeDetailsList"] = "documentExchangeDetailsDO"
    ipMetadata_Transactions_Mapping["rawDocumentDetailsList"] = "rawDataStorageDetailsList"
    ipMetadata_Transactions_Mapping["documentConsumerList"] = "documentConsumers"
    ipMetadata_Transactions_Mapping["documentIdentifierList"] = "documentIdentifiers"
    ipMetadata_Transactions_Mapping["generatedDocumentDetailsList"] = "storageAttributesList"
    ipMetadata_Transactions_Mapping["documentTags"] = "otherAttributes"
    ipMetadata_Transactions_Mapping["RequestId"] = "TenantIdTransactionId"
    ipMetadata_Transactions_Mapping["Version"] = "version"
    ipMetadata_Transactions_Mapping["RequestState"] = "state"
    ipMetadata_Transactions_Mapping["WorkflowIdentifierMap"] = "workflowId"
    ipMetadata_Transactions_Mapping["LastUpdatedTime"] = "lastUpdatedDate"
    ipMetadata_Transactions_Mapping["UsecaseIdAndVersion"] = "useCaseId"
    ipMetadata_Transactions_Mapping["DocumentMetadataList"] = "results"

    for old_name, new_name in sorted(ipMetadata_Transactions_Mapping.items()):
        try:
            dynamoDBJson = dynamoDBJson.replace(old_name, new_name)
        except:
            pass
    return dynamoDBJson

'''
The method restores the useCaseId by removing the version part
Input:
    dyanmoDBJson: Ip-Metadata record in json format
Output:
    The dynamoDBJson record with useCaseId restored
'''
def changeUseCaseId(dynamoDBJson):
    #Get useCaseId
    useCaseId = dynamoDBJson["useCaseId"]["s"]

    #Remove the version part
    i = len(useCaseId)-1
    while useCaseId[i] != ':':
        i = i - 1
    useCaseId = useCaseId[0:i]

    #restore useCaseId
    dynamoDBJson["useCaseId"]["s"] = useCaseId
    return dynamoDBJson

'''
The method restores the workflowId schema by removing the key "generateInvoiceGraph"
Input: 
    dyanmoDBJson: Ip-Metadata record in json format
Output:
    The dynamoDBJson record with restored workflowId schema
'''
def changeWorkflowId(dynamoDBJson):
    #remove the key "generateInvoiceGraph"
    try:
        dynamoDBJson["workflowId"] = dynamoDBJson["workflowId"]["m"]["generateInvoiceGraph"]
    except:
        pass
    return dynamoDBJson

'''
Removes the nested column storageAttributes from the results column of transactions record
Input: 
    dynamoDBJson: Transaction record in the form of json
Ouput:
    Transactions record with storageAttributes removed
'''
def removeStorageAttributes(dynamoDBJson):
    try:
        n = len(dynamoDBJson["results"]["l"])
        #Delete storageAttributes in every object inside results column
        for i in range(n):
            try:
                del dynamoDBJson["results"]["l"][i]["m"]["storageAttributes"]
            except:
                pass
    except:
        pass
    return dynamoDBJson


#Parameters of previous line read from mapper
previous_key = None
previous_dyanmoDBJson = None
previous_identifier = None

#total transactions records
total_transactions_records = 0
#total ip-Metadata records
total_ipMetadata_records = 0
#total matched records
matched_count = 0
#total keys not found found in ip-metadata
keyMiss_count = 0
#total unmatched records
unmatched_count = 0
#s3 write limit 
write_limit = 10

#Read every line output from mapper
for line in sys.stdin:
    
    #read current values from line 
    current_key, current_identifier_dynamoDBJson = line.split('\t')
    current_identifier, current_dynamoDBJson = current_identifier_dynamoDBJson.split('^')
    
    #Do the transformations
    if current_identifier is '1':
        total_ipMetadata_records += 1
        current_dynamoDBJson = changeColumnNames(current_dynamoDBJson)
        current_dynamoDBJson = json.loads(current_dynamoDBJson)
        current_dynamoDBJson = changeUseCaseId(current_dynamoDBJson)
        current_dynamoDBJson = changeWorkflowId(current_dynamoDBJson)        
    else:
        total_transactions_records += 1
        current_dynamoDBJson = json.loads(current_dynamoDBJson)
        current_dynamoDBJson = removeStorageAttributes(current_dynamoDBJson)
    
    #check if previous_identifer exists
    if previous_identifier is None:
        previous_key, previous_dyanmoDBJson, previous_identifier = current_key, current_dynamoDBJson, current_identifier
        continue

    #check if key is missing in Ip-Metadata records
    if current_key != previous_key:
        if keyMiss_count < write_limit:
            print('Missing from Ip-Metadata Records! Primary Key: ' + previous_key)
        keyMiss_count += 1
        previous_key, previous_dyanmoDBJson, previous_identifier = current_key, current_dynamoDBJson, current_identifier
    #check if jsons missmatch
    elif  current_dynamoDBJson != previous_dyanmoDBJson:
        if unmatched_count < write_limit:
            print('Value miss match! Key: ' + previous_key + " " + current_key )
        unmatched_count += 1
        previous_identifier = None
    #match
    else:
        matched_count += 1
        previous_identifier = None

print("Total old Records: " + str(total_transactions_records))
print("Total New Records: " + str(total_ipMetadata_records))
print("Total Key Misses: " + str(keyMiss_count))
print("Total Matches: " + str(matched_count))
print("Total Miss Matches: " + str(unmatched_count))