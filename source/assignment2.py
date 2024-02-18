from fileio import readDocuments, readStopwords
from util import PreprocessDocuments, StemDocuments
from indexer import GenerateIndexes

def __main__() :
    #Reading documents from the file
    documents = readDocuments()
    stopwords = readStopwords()
    # This does not stem the documents ( only lowercases and removes stopwords)
    processedDocuments = PreprocessDocuments(documents, stopwords)
    print('Documents are preprocessed and ready to be used for indexing')
    # This stems the documents as well along with preprocessing
    stemmedDocuments = StemDocuments(processedDocuments)
    print('Documents are stemmed along with preprocessed and ready to be used for indexing')
    indexesWithoutStem = GenerateIndexes(processedDocuments)
    print('Inverted index is generated for preprocessed dcouments')
    indexesWithStem = GenerateIndexes(stemmedDocuments)
    print('Inverted index is generated for stemmed dcouments')
    

__main__()