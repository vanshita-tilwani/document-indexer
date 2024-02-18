
# Generating inverted index for the documents
def GenerateInvertedIndex(documents):
    term_dict = {}
    for documentID in documents:
        term_dict[documentID] = {}
        for word in documents[documentID]:
            indices = [i for i, x in enumerate(documents[documentID]) if x == word]
            term_dict[documentID][word] = indices
    
    inverted_index = {}
    for documentID in term_dict:
        for word in term_dict[documentID]:
            if word not in inverted_index:
                inverted_index[word] = {}
            if documentID not in inverted_index[word]:
                inverted_index[word][documentID] = term_dict[documentID][word]

    return inverted_index