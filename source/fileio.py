import os
from concurrent.futures import ThreadPoolExecutor
from constants import Constants
from util import  ParseDocuments, ParseDocument

def readDocument(filename) :
    with open(os.path.join(Constants.DATA_PATH, filename), 'rb') as f:
        content = f.read().decode("iso-8859-1")
    return content

# Read all the documents from the directory and preprocess the documents
def readDocuments() : 
    documents = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(readDocument, filename): filename for filename in os.listdir(Constants.DATA_PATH)}
        for future in futures:
            content = future.result()
            parsedContent = ParseDocuments(content)
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(ParseDocument, document): document for document in parsedContent}
                for future in futures:
                    docID, docText = future.result()
                    documents[docID] = docText
    return documents

# Read the stopwords from the file
def readStopwords() :
    with open(Constants.STOPWORDS_PATH, 'r') as f:
        content = f.read()
        names = content.split("\n")
    return names

