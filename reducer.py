#!/usr/bin/env python3
import sys
import json


'''
The method removes the key "generateInvoiceGraph" from "WorkflowIdentifierMap" in Ip-Metadata record
Input: 
    dyanmoDBJson: Ip-Metadata record in json format
'''
def changeWorkflowIdentifierMap(dynamoDBJson):
    #remove the key "generateInvoiceGraph"
    if "WorkflowIdentifierMap" in dynamoDBJson:
        dynamoDBJson["WorkflowIdentifierMap"] = dynamoDBJson["WorkflowIdentifierMap"]["m"]["generateInvoiceGraph"]


'''
The method removes the version part from useCaseIdAndVersion in Ip-Metadata record
Input:
    dyanmoDBJson: Ip-Metadata record in json format
'''
def changeUseCaseIdAndVersion(dynamoDBJson):
    #Get useCaseId
    useCaseId = dynamoDBJson["UsecaseIdAndVersion"]["s"]

    #Remove the version part
    i = len(useCaseId)-1
    while useCaseId[i] != ':':
        i = i - 1
    useCaseId = useCaseId[0:i]

    #restore useCaseId
    dynamoDBJson["UsecaseIdAndVersion"]["s"] = useCaseId

'''
The method changes Ip-Metadata nested column names in DocumentMetadatalist to corresponding names in Transactions
Input:
    dynamoDBJson: Ip-Metadata record in json format
'''
def changeNestedColumnNamesInDocumentMetadataList(dynamoDBJson):
    '''
    Build the Reverse mapping
    Change the mapping as per the usecase
    Include only those nested columns for which mapping needs to be changed
    '''
    DocumentMetadataList_nestedColumnMapping = {}
    DocumentMetadataList_nestedColumnMapping["documentExchangeDetailsList"] = "documentExchangeDetailsDO"
    DocumentMetadataList_nestedColumnMapping["rawDocumentDetailsList"] = "rawDataStorageDetailsList"
    DocumentMetadataList_nestedColumnMapping["documentConsumerList"] = "documentConsumers"
    DocumentMetadataList_nestedColumnMapping["documentIdentifierList"] = "documentIdentifiers"
    DocumentMetadataList_nestedColumnMapping["generatedDocumentDetailsList"] = "storageAttributesList"
    DocumentMetadataList_nestedColumnMapping["documentTags"] = "otherAttributes"

    #check for existence of columns
    if "DocumentMetadataList" not in dynamoDBJson or "l" not in dynamoDBJson["DocumentMetadataList"]:
        return

    n = len(dynamoDBJson["DocumentMetadataList"]["l"])
    #iterate over every object in array
    for i in range(n):
        #check if map object exists
        if "m" in dynamoDBJson["DocumentMetadataList"]["l"][i]:
            #get the mapping for ipMetadata nested column 
            for ipMetadataName, transactionsName in DocumentMetadataList_nestedColumnMapping.items():
                #change the name
                if ipMetadataName in dynamoDBJson["DocumentMetadataList"]["l"][i]["m"]:
                    dynamoDBJson["DocumentMetadataList"]["l"][i]["m"][transactionsName] = dynamoDBJson["DocumentMetadataList"]["l"][i]["m"][ipMetadataName]
                    del dynamoDBJson["DocumentMetadataList"]["l"][i]["m"][ipMetadataName]

'''
The method changes Ip-Metadata  main column names to corresponding Transactions main column names
Input: 
    dynamoDBJson: Ip-Metadata record in json format
'''
def changeMainColumnNames(dynamoDBJson):
    '''
    Build the Reverse mapping
    Change the mapping as per the usecase
    Include only those main columns for which mapping needs to be changed
    '''
    ipMetadata_Transactions_Mapping = {}
    ipMetadata_Transactions_Mapping["RequestId"] = "TenantIdTransactionId"
    ipMetadata_Transactions_Mapping["Version"] = "version"
    ipMetadata_Transactions_Mapping["RequestState"] = "state"
    ipMetadata_Transactions_Mapping["WorkflowIdentifierMap"] = "workflowId"
    ipMetadata_Transactions_Mapping["LastUpdatedTime"] = "lastUpdatedDate"
    ipMetadata_Transactions_Mapping["UsecaseIdAndVersion"] = "useCaseId"
    ipMetadata_Transactions_Mapping["DocumentMetadataList"] = "results"

    #get the mapping for ipMetadata main column 
    for ipMetadataName, transactionsName in ipMetadata_Transactions_Mapping.items():
        #change name
        if ipMetadataName in dynamoDBJson:
            dynamoDBJson[transactionsName] = dynamoDBJson[ipMetadataName]
            del dynamoDBJson[ipMetadataName]


'''
Removes the nested column storageAttributes from the results column of Transactions record
Input: 
    dynamoDBJson: Transactions record in json format
'''
def removeStorageAttributes(dynamoDBJson):
    #check for existence of columns
    if "results" not in dynamoDBJson or "l" not in dynamoDBJson["results"]:
        return

    n = len(dynamoDBJson["results"]["l"])
    #Delete storageAttributes in every object inside results column
    for i in range(n):
        if "m" in dynamoDBJson["results"]["l"][i] and "storageAttributes" in dynamoDBJson["results"]["l"][i]["m"]:
            del dynamoDBJson["results"]["l"][i]["m"]["storageAttributes"]

#Parameters of previous line read from mapper
previous_primarykeyValue = None
previous_dyanmoDBJson = None
previous_identifier = None

#total transactions records
transactionsRecordCount = 0
#total ip-Metadata records
ipMetadataRecordCount = 0
#total data completeness failed records
dataCompletenessFailedCount = 0
#total integrity failed records
dataIntegrityFailedCount = 0
#total matched records
successCount = 0
#initialize reducer output
reducerOutput = ""

#Read every line output from mapper
for line in sys.stdin:
    
    #read current parameters from line
    current_primarykeyValue, current_identifier_dynamoDBJson = line.split('\t')
    current_identifier, current_dynamoDBJson = current_identifier_dynamoDBJson.split('#', 1)
    current_dynamoDBJson = json.loads(current_dynamoDBJson)

    #Apply Reverse Transformations to Ip-Metadata record
    if current_identifier == "RequestId":
        ipMetadataRecordCount += 1
        changeWorkflowIdentifierMap(current_dynamoDBJson)
        changeUseCaseIdAndVersion(current_dynamoDBJson)
        changeNestedColumnNamesInDocumentMetadataList(current_dynamoDBJson)
        changeMainColumnNames(current_dynamoDBJson)        
    #Remove storage Attributes from Transactions record
    else:
        transactionsRecordCount += 1
        removeStorageAttributes(current_dynamoDBJson)
    
    #check if previous_identifer exists
    if previous_identifier is None:
        previous_primarykeyValue, previous_dyanmoDBJson, previous_identifier = current_primarykeyValue, current_dynamoDBJson, current_identifier
        continue
        
    #Data completeness failure
    if current_primarykeyValue != previous_primarykeyValue:
        reducerOutput += "Data completenss failed at key: " + previous_primarykeyValue + "\n"
        dataCompletenessFailedCount += 1
        previous_primarykeyValue, previous_dyanmoDBJson, previous_identifier = current_primarykeyValue, current_dynamoDBJson, current_identifier
    #Data integrity failure
    elif  current_dynamoDBJson != previous_dyanmoDBJson:
        reducerOutput += "Data integrity failed at key: " + previous_primarykeyValue + "\n"
        dataIntegrityFailedCount += 1
        previous_identifier = None
    #Matching success
    else:
        successCount += 1
        previous_identifier = None
    
print("Transactions records read: " + str(transactionsRecordCount))
print("Ip-Metadata records read: " + str(ipMetadataRecordCount))
print("Data completeness failures: " + str(dataCompletenessFailedCount))
print("Data integrity failures: " + str(dataIntegrityFailedCount))
print("Successfully matched records: " + str(successCount))
if  dataCompletenessFailedCount == 0 and dataIntegrityFailedCount == 0:
    print(reducerOutput, end = "")
