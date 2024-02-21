from fileio import readDocuments, readStopwords, readFileAsJson, readQueries, write, readFileAsDictionary
from util import PreprocessDocuments, StemDocuments, ProcessQueries, TokenizeDocumentIds
from indexer import index, IndexExists
from constants import Constants
from retrievalmodel import Model
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import os


def __main__() :
    documents = readDocuments()
    stopwords = readStopwords()
    document_mapping, unstemmed_index, stemmed_index = __generateIndexes(documents, stopwords)

    queries = readQueries()
    processedQueries = ProcessQueries(queries, stopwords)
    models = [Constants.OKAPI_TF, Constants.TF_IDF, Constants.BM_25, Constants.LM_LAPLACE, Constants.LM_JELINEKMERCER]
    __executeQueries(processedQueries, models, document_mapping, unstemmed_index)
    __executeQueries(processedQueries, models, document_mapping, stemmed_index)
    print('Inverted index is generated for both stemmed and unstemmed documents')

def __executeQueries(queries, models, document, index) :
    for query in queries:
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(__executeQuery, queries[query], model, document, index): model for model in models}
            for future in as_completed(futures):
                try:
                    pass
                except Exception as exc:
                    print(f"Query execution generated an exception: {exc}")
                print('Executing the query on specified model')

def __executeQuery(query, model, document, index) :
    retrieval_model = Model(model, index, query, document)
    retrieval_model.Execute()
    print('Executing the query on specified model')

# This function reads the documents from the file and generates the inverted index for the documents if it does not exist
# If index already exists, it reads the catalog from the file for both stemmed and unstemmed documents
def __generateIndexes(documents, stopwords) :
    
    doesUnstemmedIndexExists = IndexExists(Constants.INDEX_TYPE_UNSTEMMED)
    doesStemmedIndexExists = IndexExists(Constants.INDEX_TYPE_STEMMED)
    if(doesUnstemmedIndexExists and doesStemmedIndexExists) :
        return readFileAsDictionary('config', Constants.DOCUMENT_MAPPING_FILE_NAME), readFileAsJson(Constants.INDEX_TYPE_UNSTEMMED), readFileAsJson(Constants.INDEX_TYPE_STEMMED)
    
    document_mapping, tokenizedDocuments = TokenizeDocumentIds(documents)
    write('config', Constants.DOCUMENT_MAPPING_FILE_NAME, document_mapping)
    processedDocuments = PreprocessDocuments(tokenizedDocuments, stopwords)
    unstemmed_index = __getInvertedIndex(doesUnstemmedIndexExists, Constants.INDEX_TYPE_UNSTEMMED, processedDocuments)
    stemmed_index = __getInvertedIndex(doesStemmedIndexExists, Constants.INDEX_TYPE_STEMMED, processedDocuments)
    return document_mapping, unstemmed_index, stemmed_index

def __getInvertedIndex(indexExists, type, documents) :
    if indexExists:
        indexes = readFileAsJson(type)
    else :
        __cleanup(type)
        if type == Constants.INDEX_TYPE_STEMMED:
            documents = StemDocuments(documents)
        indexes = index(type, documents)
    return indexes

def __cleanup(type) :
    path = Constants.OUTPUT_PATH + '/' + type + '/*'
    files = glob.glob(path)
    for f in files:
        os.remove(f)

__main__()