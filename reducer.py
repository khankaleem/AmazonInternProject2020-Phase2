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
    useCaseId = useCaseId.rsplit(":", 1)[0]
    #restore useCaseId
    dynamoDBJson["UsecaseIdAndVersion"]["s"] = useCaseId

'''
The method changes Ip-Metadata nested column names in DocumentMetadatalist to corresponding names in Transactions
Input:
    dynamoDBJson: Ip-Metadata record in json format
'''
def changeNestedColumnNamesInDocumentMetadataList(dynamoDBJson):

    #check for existence of column
    if "DocumentMetadataList" not in dynamoDBJson:
        return

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


    '''
    The method changes names of columns inside the arrayIndex-th object in DocumentMetadataList
    Input: 
        arrayIndex: The index of the object in the array DocumentMetadataList
    '''
    def changeColumnNamesInArrayObject(arrayIndex):
        #change all the required names in the arrayIndex-th object in DocumentMetadataList
        for ipMetadataName, transactionsName in DocumentMetadataList_nestedColumnMapping.items():
            if ipMetadataName in dynamoDBJson["DocumentMetadataList"]["l"][arrayIndex]["m"]:
                #assign the value at key ipMetadataName, to key transactionsName, and delete the former key
                dynamoDBJson["DocumentMetadataList"]["l"][arrayIndex]["m"][transactionsName] = dynamoDBJson["DocumentMetadataList"]["l"][arrayIndex]["m"][ipMetadataName]
                del dynamoDBJson["DocumentMetadataList"]["l"][arrayIndex]["m"][ipMetadataName]

    #get length of DocumentMetadataList array
    n = len(dynamoDBJson["DocumentMetadataList"]["l"])
    #iterate over every object in the array DocumentMetadataList
    for arrayIndex in range(n):
        #check if map object exists inside arrayIndex-th object
        if "m" in dynamoDBJson["DocumentMetadataList"]["l"][arrayIndex]:
            #change column names in object
            changeColumnNamesInArrayObject(arrayIndex)


'''
The method changes Ip-Metadata  outer column names to corresponding Transactions main column names
Input: 
    dynamoDBJson: Ip-Metadata record in json format
'''
def changeOuterColumnNames(dynamoDBJson):
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

    #Iterate over the mapping and change the names
    for ipMetadataName, transactionsName in ipMetadata_Transactions_Mapping.items():
        if ipMetadataName in dynamoDBJson:
            #assign the value at key ipMetadataName, to key transactionsName, and delete the former key
            dynamoDBJson[transactionsName] = dynamoDBJson[ipMetadataName]
            del dynamoDBJson[ipMetadataName]

'''
Removes the nested column storageAttributes from the results column of Transactions record
Input: 
    dynamoDBJson: Transactions record in json format
'''
def removeStorageAttributes(dynamoDBJson):
    #check for existence of columns
    if "results" not in dynamoDBJson:
        return
    #get length of results array
    n = len(dynamoDBJson["results"]["l"])
    #Delete storageAttributes in every object inside results column
    for i in range(n):
        if "m" in dynamoDBJson["results"]["l"][i] and "storageAttributes" in dynamoDBJson["results"]["l"][i]["m"]:
            del dynamoDBJson["results"]["l"][i]["m"]["storageAttributes"]

#Initialize parameters
'''
The variable signifies the value of the primary key,  
read from previous item.
'''
previousPrimarykeyValue = None

'''
The variable signifies the DynamoDB Json of previous 
item read.
'''
previousDyanmoDBJson = None

'''
The varaible signifies if the parameters of current line 
need to be compared to the parameters of previous line.
'''
isComparisonRequiredWithPreviousPrimaryKeyValue = False


'''
Initialize the count variables to be displayed on the master shell under the title/group Report.
The following line of code increases the count of 'counterName' in group 'groupName' by x:
    sys.stderr.write("reporter:counter:groupName,counterName,x")
'''

'''
transactionsRecordCount:
    The variable stores the total transaction records read.
'''
sys.stderr.write("reporter:counter:Report,transactionsRecordCount,0\n")

'''
ipMetadataRecordCount:
    The variable signifies the total Ip-Metadata records read.
'''
sys.stderr.write("reporter:counter:Report,ipMetadataRecordCount,0\n")

'''
dataCompletenessFailedCount:
    The variable signifies the number of transaction records
    and ip-Metadata records for which a matching primary key 
    value was not found.
'''
sys.stderr.write("reporter:counter:Report,dataCompletenessFailedCount,0\n")

'''
dataIntegrityFailedCount:
    The variable signifies the number of transaction records 
    for which the primary key value matched, but the DynamoDB 
    Json objects did not match.
'''
sys.stderr.write("reporter:counter:Report,dataIntegrityFailedCount,0\n")

'''
dataCompletenessAndIntegritySuccessCount:
    The variable signifies the number of transaction records 
    which successfully got matched to Ip-metadata records.
'''
sys.stderr.write("reporter:counter:Report,dataCompletenessAndIntegritySuccessCount,0\n")

#Read every line output from mapper
for line in sys.stdin:
    
    #read current parameters from line
    currentPrimarykeyValue, currentDynamoDBJson = line.split('\t')
    #convert dynamoDBJson to Json object
    currentDynamoDBJson = json.loads(currentDynamoDBJson)

    #Apply Reverse Transformations to Ip-Metadata record
    if "RequestId" in currentDynamoDBJson:
        sys.stderr.write("reporter:counter:Report,ipMetadataRecordCount,1\n")
        changeWorkflowIdentifierMap(currentDynamoDBJson)
        changeUseCaseIdAndVersion(currentDynamoDBJson)
        changeNestedColumnNamesInDocumentMetadataList(currentDynamoDBJson)
        changeOuterColumnNames(currentDynamoDBJson)
    #Remove storage Attributes from Transactions record
    else:
        sys.stderr.write("reporter:counter:Report,transactionsRecordCount,1\n")
        removeStorageAttributes(currentDynamoDBJson)
    
    #check if previous_identifer exists
    if isComparisonRequiredWithPreviousPrimaryKeyValue is False:
        previousPrimarykeyValue, previousDyanmoDBJson, isComparisonRequiredWithPreviousPrimaryKeyValue = currentPrimarykeyValue, currentDynamoDBJson, True
        continue
        
    #Data completeness failure: Matching key for primary key value of previous item not found
    if currentPrimarykeyValue != previousPrimarykeyValue:
        print("Data completeness failed at key: " + previousPrimarykeyValue)
        sys.stderr.write("reporter:counter:Report,dataCompletenessFailedCount,1\n")
        previousPrimarykeyValue, previousDyanmoDBJson, isComparisonRequiredWithPreviousPrimaryKeyValue = currentPrimarykeyValue, currentDynamoDBJson, True
    #Data integrity failure: Primary key values match but DynamoDB jsons dont match
    elif  currentDynamoDBJson != previousDyanmoDBJson:
        print("Data integrity failed at key: " + previousPrimarykeyValue)
        sys.stderr.write("reporter:counter:Report,dataIntegrityFailedCount,1\n")
        isComparisonRequiredWithPreviousPrimaryKeyValue = False
    #Both primary key values and DynamoDB jsons match
    else:
        sys.stderr.write("reporter:counter:Report,dataCompletenessAndIntegritySuccessCount,1\n")
        isComparisonRequiredWithPreviousPrimaryKeyValue = False

#Check if an unmatched primary key value is left
if isComparisonRequiredWithPreviousPrimaryKeyValue is True:
    print("Data completeness failed at key: " + previousPrimarykeyValue)
    sys.stderr.write("reporter:counter:Report,dataCompletenessFailedCount,1\n")
