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

    partial_list_catalog = {}

    def _processBatch(index, batch, type) :
        invertedIndex = __generateInvertedIndex(batch)
        for term in invertedIndex[Constants.TERM_INDEX]:
            filename = 'inverted_index' + str(index) + '.txt'
            startOffset = currentOffset(type, filename)
            write(type, 'inverted_index' + str(index) + '.txt' , invertedIndex[Constants.TERM_INDEX][term])
            endOffset = currentOffset(type, filename)
            if term not in partial_list_catalog:
                partial_list_catalog[term] = []
            partial_list_catalog[term].append({'path' : type, 'filename' : filename, 'start' : startOffset, 'size' : endOffset - startOffset})

    total_batches = [dict(list(documents.items())[i:i+Constants.BATCH_SIZE]) for i in range(0, len(documents), Constants.BATCH_SIZE)]
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(_processBatch, index, batch, type): (index, batch) for index, batch in enumerate(total_batches)}
        for future in as_completed(futures):
            index, batch = futures[future]
            try:
                pass
            except Exception as exc:
                print(f"Processing failed for batch {index}: {exc}")

    catalog = {}
    for term in partial_list_catalog:
        invertedIndexByTerm = {}
        for index in range(0, len(partial_list_catalog[term])):
            currentPartialList = partial_list_catalog[term][index]
            data = seekFile(currentPartialList['path'], currentPartialList['filename'], currentPartialList['start'], currentPartialList['size'])
            partialIndexByTerm = ast.literal_eval(data)
            invertedIndexByTerm = __mergeInvertedIndexes(invertedIndexByTerm, partialIndexByTerm)
        startOffset = currentOffset(type, 'inverted_index.txt')
        write(type, 'inverted_index.txt', invertedIndexByTerm)
        endOffset = currentOffset(type, 'inverted_index.txt')
        catalog[term] = {'path' : type, 'filename' : 'inverted_index.txt', 'start' : startOffset, 'size' : endOffset - startOffset}
    
    write(type, 'catalog.json', catalog)
    return catalog

# Generate the inverted index for the given documents
def __generateInvertedIndex(documents):
    global vocabulary
    global corpusSize
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

    return {
        Constants.DOCUMENT_INDEX : documentIndex,
        Constants.TERM_INDEX : termIndex,
        Constants.VOCABULARY_SIZE : len(vocabulary),
        Constants.CORPUS_SIZE : corpusSize
    }

def __mergeInvertedIndexes(indexes, partial):
    for document in partial :
        if document in indexes:
            indexes[document].extend(partial[document])
        else:
            indexes[document] = partial[document]
    return indexes

vocabulary = set() # Set of all unique terms in the corpus
corpusSize = 0 # Total number of terms in the corpus