from fileio import readDocuments, readStopwords, readCatalog
from util import PreprocessDocuments, StemDocuments
from indexer import index, IndexExists
from constants import Constants
import glob
import os
import copy


def __main__() :
    unstemmed_index, stemmed_index = __getInvertedIndexes()
    print('Inverted index is generated for both stemmed and unstemmed documents')

# This function reads the documents from the file and generates the inverted index for the documents if it does not exist
# If index already exists, it reads the catalog from the file for both stemmed and unstemmed documents
def __getInvertedIndexes() :
    doesUnstemmedIndexExists = IndexExists(Constants.INDEX_TYPE_UNSTEMMED)
    doesStemmedIndexExists = IndexExists(Constants.INDEX_TYPE_STEMMED)
    processedDocuments = _processDocuments(doesStemmedIndexExists, doesUnstemmedIndexExists)
    
    unstemmed_index = __getInvertedIndex(doesUnstemmedIndexExists, Constants.INDEX_TYPE_UNSTEMMED, processedDocuments)
    stemmed_index = __getInvertedIndex(doesStemmedIndexExists, Constants.INDEX_TYPE_STEMMED, processedDocuments)
    return unstemmed_index, stemmed_index

def __getInvertedIndex(indexExists, type, documents) :
    if indexExists:
        indexes = readCatalog(type)
    else :
        indexes = __indexDocuments(type, documents)
    return indexes

def __indexDocuments(type, documents) :
     # Cleaning up the output directory
    __cleanup(type)
    final_documents = copy.deepcopy(documents)
    if type == Constants.INDEX_TYPE_STEMMED:
        final_documents = StemDocuments(final_documents)
    indexes = index(type, final_documents)
    return indexes

def _processDocuments(stemmed, unstemmed) :
    if stemmed and unstemmed:
        return {}
    documents = readDocuments()
    stopwords = readStopwords()
    processedDocuments = PreprocessDocuments(documents, stopwords)
    return processedDocuments

def __cleanup(type) :
    path = Constants.OUTPUT_PATH + '/' + type + '/*'
    files = glob.glob(path)
    for f in files:
        os.remove(f)

__main__()