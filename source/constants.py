class Constants:
    DATA_PATH = '/Users/vanshitatilwani/Documents/Courses_Local/CS6200/hw2-vanshita-tilwani/IR_data/AP_DATA/ap89_collection'
    STOPWORDS_PATH = '/Users/vanshitatilwani/Documents/Courses_Local/CS6200/hw2-vanshita-tilwani/resources/stoplist.txt'
    RESOURCES_PATH = '/Users/vanshitatilwani/Documents/Courses_Local/CS6200/hw2-vanshita-tilwani/resources'
    OUTPUT_PATH = '/Users/vanshitatilwani/Documents/Courses_Local/CS6200/hw2-vanshita-tilwani/output'
    QUERY_PATH = '/Users/vanshitatilwani/Documents/Courses_Local/CS6200/hw2-vanshita-tilwani/resources/modified_queries.txt'
    OKAPI_TF = 'model_okapi_tf'
    TF_IDF = 'model_tfidf'
    BM_25 = 'model_bm25'
    LM_LAPLACE = 'model_lm_laplace'
    LM_JELINEKMERCER = 'model_lm_jk'
    BM25_K1 = 1.2
    BM25_B = 0.75
    BM25_K2 = 0
    CORPUS_PROB = 0.99
    
    INDEX_TYPE_STEMMED = 'stemmed'
    INDEX_TYPE_UNSTEMMED = 'unstemmed'
    INDEX_FILE_NAME = 'inverted_index'
    CATALOG_FILE_NAME = 'catalog'
    DOCUMENT_MAPPING_FILE_NAME = 'document_mapping'
    COMMA = ','
    APOSTROPHE = '\''
    PUNCTUATIONS = '.,!?;()[]\{\}<>/\|@#$%^&*-_+=~`"'
    BATCH_SIZE : int = 1000
    DOCUMENT_INDEX = 'DOCUMENT_INDEX'
    TERM_INDEX = 'TERM_INDEX'
    VOCABULARY_SIZE = 'VOCABULARY_SIZE'
    CORPUS_SIZE = 'CORPUS_SIZE'
