from fileio import readDocuments, readStopwords, readQueries, WriteToResults, read, seekBinary
from util import  StemData, ProcessQueries
from indexer import GenerateIndexes
from constants import Constants
from retrievalmodel import Model
import os

def __main__() :

    # Reading documents and stopwords from resources
    documents = readDocuments()
    stopwords = readStopwords()
    # Reading the indexes if already exists, if the indexes are not present then generating new indexes
    document_mapping, unstemmed_decompressed_index, unstemmed_compressed_index, stemmed_decompressed_index, stemmed_compressed_index = GenerateIndexes(documents, stopwords)
    print('Inverted index is generated for both stemmed and unstemmed documents')

    # Reading queries from the resources
    queries = readQueries()
    # Processing the queries, i.e. tokenizing and stopword removal
    processedQueries = ProcessQueries(queries, stopwords)
    # Stemming the queries
    stemmedQueries = StemData(processedQueries)

    print('Queries are processed and ready to be used for running on each model')
    # Models to run retrieval models on
    models = [Constants.OKAPI_TF, Constants.TF_IDF, Constants.BM_25, Constants.LM_JELINEKMERCER, Constants.LM_LAPLACE, Constants.PROXIMITY_SEARCH]
    # Executing Queries for the models on unstemmed index
    __executeQueries(Constants.INDEX_TYPE_UNSTEMMED, Constants.DECOMPRESSED_INDEX, processedQueries, models, document_mapping, unstemmed_decompressed_index)
    __executeQueries(Constants.INDEX_TYPE_UNSTEMMED, Constants.COMPRESSED_INDEX, processedQueries, models, document_mapping, unstemmed_compressed_index)
    
    # Executing Queries for the models on stemmed index
    __executeQueries(Constants.INDEX_TYPE_STEMMED, Constants.DECOMPRESSED_INDEX, stemmedQueries, models, document_mapping, stemmed_decompressed_index)
    __executeQueries(Constants.INDEX_TYPE_STEMMED, Constants.COMPRESSED_INDEX, stemmedQueries, models, document_mapping, stemmed_compressed_index)
    
    print('The results for each of the model is generated in the output directory')
    

def __executeQueries(index_type, index_size, queries, models, document_mapping, index) :
    # Deleting result file before running queries
    for model in models :
        __deleteResultFiles(index_type, index_size, model)

    # Creating the model object with index and documents 
    retrieval_model = Model(index_size, index, document_mapping)
    for query in queries:
        for model in models:
            # Setting model type to use to run queries
            retrieval_model.SetModel(model)
            # Setting the query which will be used to run on the model
            retrieval_model.SetQuery(queries[query])
            # Executing query on the model
            __executeQuery(index_type, index_size, retrieval_model, document_mapping, query,  model)

def __executeQuery(index_type, index_size, retrieval_model, document_mapping, query, model) :
    # Execute the query on the model using the index
    score = retrieval_model.Execute()
    WriteToResults(index_type + '/' + index_size, model, query, score, document_mapping)

def __deleteResultFiles(index_type, index_size, model) :
    file_path =  Constants.OUTPUT_PATH + '/' + index_type + '/' + index_size + '/' + model + '.txt'
    if(os.path.exists(file_path)):
        os.remove(file_path)

__main__()