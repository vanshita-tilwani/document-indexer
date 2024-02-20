from constants import Constants
from fileio import write, currentOffset, seekFile
from concurrent.futures import ThreadPoolExecutor,as_completed
import ast

# Generating inverted index for the documents
# This stores the inverted index in the following format:
# documentID -> {term -> [list of positions of the term in the document]}
# term -> {documentID -> [list of positions of the term in the document]}
# DF = number of keys in the dictionary for a term, TTF = sum of the length of the list of positions for a term
# Type denotes the type of the document, which can be stemmed or unstemmed
def GenerateInvertedIndex(type, documents):
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
            data = seekFile(currentPartialList['path'], currentPartialList['filename'], currentPartialList['start'], currentPartialList['size'])
            partialIndexByTerm = ast.literal_eval(data)
            invertedIndexByTerm = __mergeInvertedIndexes(invertedIndexByTerm, partialIndexByTerm)
        startOffset = currentOffset(type, main_index_file)
        write(type, main_index_file, invertedIndexByTerm)
        endOffset = currentOffset(type, main_index_file)
        catalog[term] = {'path' : type, 'filename' : main_index_file, 'start' : startOffset, 'size' : endOffset - startOffset}
    
    write(type, Constants.CATALOG_FILE_NAME + '.txt', catalog)
    return catalog


def __writePartialInvertedIndex(term, type, filename, invertedIndex):
    startOffset = currentOffset(type, filename)
    write(type, filename , invertedIndex[term])
    endOffset = currentOffset(type, filename)
    return startOffset, endOffset - startOffset

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
        
        partial_catalog_filename = Constants.CATALOG_FILE_NAME + str((index+1)) + '.txt'
        write(type, partial_catalog_filename, partial_catalog)

# Generate the inverted index for the given documents
def __generateInvertedIndex(documents):
    
    documentIndex = {}
    for documentID in documents:
        documentIndex[documentID] = {}
        for word in documents[documentID]:
            indices = [i for i, x in enumerate(documents[documentID]) if x == word]
            documentIndex[documentID][word] = indices
            vocabulary.add(word)
            corpusSize += 1
    
    termIndex = {}
    for documentID in documentIndex:
        for word in documentIndex[documentID]:
            if word not in termIndex:
                termIndex[word] = {}
            if documentID not in termIndex[word]:
                termIndex[word][documentID] = documentIndex[documentID][word]

    return termIndex

def __mergeInvertedIndexes(indexes, partial):
    for document in partial :
        if document in indexes:
            indexes[document].extend(partial[document])
        else:
            indexes[document] = partial[document]
    return indexes