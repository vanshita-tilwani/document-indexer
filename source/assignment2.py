from fileio import readDocuments, readStopwords
from util import PreprocessDocuments, StemDocuments
import time


def __main__() :
    #Reading documents from the file
    documents = readDocuments()
    stopwords = readStopwords()
    processedDocuments = PreprocessDocuments(documents, stopwords)
    stemmedDocuments = StemDocuments(processedDocuments)
    print('Documents are tokenized and ready to be used for indexing')

__main__()