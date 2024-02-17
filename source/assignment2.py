from fileio import readDocuments, readStopwords
from util import PreprocessDocuments

def __main__() :
    #Reading documents from the file
    documents = readDocuments()
    stopwords = readStopwords()
    processedDocuments = PreprocessDocuments(documents)
    print('Documents are tokenized and ready to be used for indexing')
__main__()