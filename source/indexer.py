from constants import Constants
from fileio import write, offset, seek, read, Cleanup
from util import PreprocessDocuments, StemData, TokenizeDocumentIds
from concurrent.futures import ThreadPoolExecutor,as_completed

# Generating inverted index for the documents
# This stores the inverted index in the following format:
# documentID -> {term -> [list of positions of the term in the document]}
# term -> {documentID -> [list of positions of the term in the document]}
# DF = number of keys in the dictionary for a term, TTF = sum of the length of the list of positions for a term
# Type denotes the type of the document, which can be stemmed or unstemmed
def GenerateIndexes(documents, stopwords) :
    doesUnstemmedIndexExists = IndexExists(Constants.INDEX_TYPE_UNSTEMMED)
    doesStemmedIndexExists = IndexExists(Constants.INDEX_TYPE_STEMMED)
    if(doesUnstemmedIndexExists and doesStemmedIndexExists) :
        return __readInvertedIndex()
    else :
        userRegerateIndexes = __regenerateIndexes()
        if(userRegerateIndexes):
            return __generateIndex(False, False, documents, stopwords)
        else:
            return __generateIndex(doesUnstemmedIndexExists, doesStemmedIndexExists, documents, stopwords)

# Checks if the index exists for the type
def IndexExists(type):
    catalog = read(type, Constants.CATALOG_FILE_NAME)
    if len(catalog) > 0:
        return True
    else:
        return False

# Reads the inverted index from the file
def TermVector(catalog, term):
    catalog_data = catalog[term]
    term_vector = seek(catalog_data['path'], catalog_data['filename'], catalog_data['start'], catalog_data['size'])
    return term_vector

# This function processes the documents in a batch and generates the inverted index for the batch and merges it to form main inverted index
def __index(type, documents):
    #split the document into batches of 1000
    catalog = {}
    total_batches = [dict(list(documents.items())[i:i+Constants.BATCH_SIZE]) for i in range(0, len(documents), Constants.BATCH_SIZE)]
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(__processBatch, catalog, index, batch, type): (index, batch) for index, batch in enumerate(total_batches)}
        for future in as_completed(futures):
            index, batch = futures[future]
            try:
                pass
            except Exception as exc:
                print(f"Processing failed for batch {index}: {exc}")


    main_index_file =  Constants.INDEX_FILE_NAME + '.txt'
    for term in catalog:
        invertedIndexByTerm = {}
        for index in range(0, len(catalog[term])):
            currentPartialList = catalog[term][index]
            partialIndexByTerm = seek(currentPartialList['path'], currentPartialList['filename'], currentPartialList['start'], currentPartialList['size'])
            invertedIndexByTerm = __mergeInvertedIndexes(invertedIndexByTerm, partialIndexByTerm)
        startOffset = offset(type, main_index_file)
        write(type, main_index_file, invertedIndexByTerm)
        endOffset = offset(type, main_index_file)
        catalog[term] = {'path' : type, 'filename' : main_index_file, 'start' : startOffset, 'size' : endOffset - startOffset}
    
    write(type, Constants.CATALOG_FILE_NAME + '.json', catalog, False)
    return catalog

# Write the inverted index to the file
def __writePartialInvertedIndex(term, type, filename, invertedIndex):
    startOffset = offset(type, filename)
    write(type, filename , invertedIndex[term])
    endOffset = offset(type, filename)
    return startOffset, endOffset - startOffset

# Prepares the inverted index and catalog for the given batch of 1000 documents
def __processBatch(catalog, index, batch, type) : 
    invertedIndex = __generateInvertedIndex(batch)
    partial_catalog = {}
    for term in invertedIndex:
        # Write the inverted index to the file
        partial_index_filename = Constants.INDEX_FILE_NAME + str((index+1)) + '.txt'
        startOffset, size = __writePartialInvertedIndex(term, type, partial_index_filename, invertedIndex)
        if term not in catalog:
            catalog[term] = []
        catalog[term].append({'path' : type, 'filename' : partial_index_filename, 'start' : startOffset, 'size' : size})
            
        if term not in partial_catalog:
            partial_catalog[term] = []
        partial_catalog[term].append({'path' : type, 'filename' : partial_index_filename, 'start' : startOffset, 'size' : size})
        
    partial_catalog_filename = Constants.CATALOG_FILE_NAME + str((index+1)) + '.json'
    write(type, partial_catalog_filename, partial_catalog, False)

# Generate the inverted index for the given documents
def __generateInvertedIndex(documents):
    documentIndex = {}
    for documentID in documents:
        documentIndex[documentID] = {}
        for word in documents[documentID]:
            indices = [i for i, x in enumerate(documents[documentID]) if x == word]
            documentIndex[documentID][word] = indices
    
    termIndex = {}
    for documentID in documentIndex:
        for word in documentIndex[documentID]:
            if word not in termIndex:
                termIndex[word] = {}
            if documentID not in termIndex[word]:
                termIndex[word][documentID] = documentIndex[documentID][word]

    return termIndex

# Merge the partial inverted indexes to form the main inverted index
def __mergeInvertedIndexes(indexes, partial):
    for document in partial :
        if document in indexes:
            indexes[document].extend(partial[document])
        else:
            indexes[document] = partial[document]
    return indexes

# Reads the inverted index for both stemmed and unstemmed documents from the file
def __readInvertedIndex() :
    document_meta = read('config', Constants.DOCUMENT_MAPPING_FILE_NAME) 
    document_mapping = {int(key):document_meta[key] for key in document_meta}
    unstemmed_index = read(Constants.INDEX_TYPE_UNSTEMMED, Constants.CATALOG_FILE_NAME)
    stemmed_index = read(Constants.INDEX_TYPE_STEMMED, Constants.CATALOG_FILE_NAME)
    return document_mapping, unstemmed_index, stemmed_index

# Preprocesses the documents and Generates the inverted index for stemmed and unstemmed documents
def __generateIndex(doesUnstemmedIndexExists, doesStemmedIndexExists, documents, stopwords) :
    document_mapping, tokenizedDocuments = TokenizeDocumentIds(documents)
    processedDocuments = PreprocessDocuments(document_mapping, tokenizedDocuments, stopwords)
    write('config', Constants.DOCUMENT_MAPPING_FILE_NAME + '.json', document_mapping, False)

    unstemmed_index = __getInvertedIndex(doesUnstemmedIndexExists, Constants.INDEX_TYPE_UNSTEMMED, processedDocuments)
    stemmed_index = __getInvertedIndex(doesStemmedIndexExists, Constants.INDEX_TYPE_STEMMED, processedDocuments)
    return document_mapping, unstemmed_index, stemmed_index

# Gets the inverted index for the given type (i.e. stemmed or unstemmed)
def __getInvertedIndex(indexExists, type, documents) :
    if indexExists:
        indexes = read(type)
    else :
        Cleanup(type)
        if type == Constants.INDEX_TYPE_STEMMED:
            documents = StemData(documents)
        indexes = __indexDocuments(type, documents)
    return indexes

# Indexes the documents and writes the inverted index to the file
def __indexDocuments(type, documents) :
    catalog = {}
    if type == Constants.INDEX_TYPE_STEMMED or type == Constants.INDEX_TYPE_UNSTEMMED :
        catalog = __index(type, documents)
    else :
        raise ValueError('Invalid index type')
    print('Inverted index is generated for ' + type + ' documents')
    return catalog

def __regenerateIndexes() :
    answer = input('Do you wish to re-generate indexes? [yes/no]')
    if answer.lower() == 'yes' or answer.lower() == 'y':
        return True
    else:
        return False