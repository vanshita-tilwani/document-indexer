from constants import Constants

# Generating inverted index for the documents
# This stores the inverted index in the following format:
# term -> {documentID -> [list of positions of the term in the document]}
# DF = number of keys in the dictionary for a term, TTF = sum of the length of the list of positions for a term
def GenerateIndexes(documents):
    vocabulary = set() # Set of all unique terms in the corpus
    corpusSize = 0 # Total number of terms in the corpus
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