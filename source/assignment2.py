from fileio import readDocuments, readStopwords, readCatalog, readQueries
from util import PreprocessDocuments, StemDocuments, ProcessQueries
from indexer import index, IndexExists
from constants import Constants
import glob
import os
import copy


def __main__() :
    documents = readDocuments()
    stopwords = readStopwords()
    unstemmed_index, stemmed_index = __generateIndexes(documents, stopwords)

    queries = readQueries()
    processedQueries = ProcessQueries(queries, stopwords)
    print('Inverted index is generated for both stemmed and unstemmed documents')

# This function reads the documents from the file and generates the inverted index for the documents if it does not exist
# If index already exists, it reads the catalog from the file for both stemmed and unstemmed documents
def __generateIndexes(documents, stopwords) :
    
    doesUnstemmedIndexExists = IndexExists(Constants.INDEX_TYPE_UNSTEMMED)
    doesStemmedIndexExists = IndexExists(Constants.INDEX_TYPE_STEMMED)
    if(doesUnstemmedIndexExists and doesStemmedIndexExists) :
        return readCatalog(Constants.INDEX_TYPE_UNSTEMMED), readCatalog(Constants.INDEX_TYPE_STEMMED)
    
    processedDocuments = PreprocessDocuments(documents, stopwords)
    unstemmed_index = __getInvertedIndex(doesUnstemmedIndexExists, Constants.INDEX_TYPE_UNSTEMMED, processedDocuments)
    stemmed_index = __getInvertedIndex(doesStemmedIndexExists, Constants.INDEX_TYPE_STEMMED, processedDocuments)
    return unstemmed_index, stemmed_index

def __getInvertedIndex(indexExists, type, documents) :
    if indexExists:
        indexes = readCatalog(type)
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