from fileio import readDocuments, readStopwords
from util import PreprocessDocuments, StemDocuments
from indexer import GenerateInvertedIndex
from constants import Constants
import glob
import os
import copy


def __main__() :
    # Cleaning up the output directory
    cleanup()
    #Reading documents from the file
    documents = readDocuments()
    stopwords = readStopwords()
    # This does not stem the documents ( only lowercases and removes stopwords)
    processedDocuments = PreprocessDocuments(copy.deepcopy(documents), stopwords)
    print('Documents are preprocessed and ready to be used for indexing')
    # This stems the documents as well along with preprocessing
    indexesWithoutStem = GenerateInvertedIndex(Constants.INDEX_TYPE_UNSTEMMED, copy.deepcopy(processedDocuments))
    print('Inverted index is generated for preprocessed dcouments')
    stemmedDocuments = StemDocuments(copy.deepcopy(processedDocuments))
    print('Documents are stemmed along with preprocessed and ready to be used for indexing')
    indexesWithStem = GenerateInvertedIndex(Constants.INDEX_TYPE_STEMMED, copy.deepcopy(stemmedDocuments))
    print('Inverted index is generated for stemmed dcouments')

def cleanup() :
    folders = [Constants.OUTPUT_PATH + '/' + Constants.INDEX_TYPE_UNSTEMMED +  '/*', Constants.OUTPUT_PATH + '/' + Constants.INDEX_TYPE_STEMMED +  '/*']
    for path in folders:
        files = glob.glob(path)
        for f in files:
            os.remove(f)

__main__()