from fileio import readDocuments, readStopwords, readQueries, WriteToResults
from util import  StemData, ProcessQueries
from indexer import GenerateIndexes
from constants import Constants
from retrievalmodel import Model

def __main__() :
    documents = readDocuments()
    stopwords = readStopwords()
    document_mapping, unstemmed_index, stemmed_index = GenerateIndexes(documents, stopwords)

    queries = readQueries()
    processedQueries = ProcessQueries(queries, stopwords)
    stemmedQueries = StemData(processedQueries)
    models = [Constants.OKAPI_TF, Constants.TF_IDF, Constants.BM_25, Constants.LM_LAPLACE, Constants.LM_JELINEKMERCER]
    __executeQueries(Constants.INDEX_TYPE_UNSTEMMED, processedQueries, models, document_mapping, unstemmed_index)
    __executeQueries(Constants.INDEX_TYPE_STEMMED, stemmedQueries, models, document_mapping, stemmed_index)
    print('Inverted index is generated for both stemmed and unstemmed documents')

def __executeQueries(index_type, queries, models, document_mapping, index) :
    retrieval_model = Model(index, document_mapping)
    for query in queries:
        for model in models:
            retrieval_model.SetModel(model)
            retrieval_model.SetQuery(queries[query])
            __executeQuery(index_type, retrieval_model, document_mapping, query,  model)

def __executeQuery(index_type, retrieval_model, document_mapping, query, model) :
    score = retrieval_model.Execute()
    WriteToResults(index_type, model, query, score, document_mapping)

__main__()