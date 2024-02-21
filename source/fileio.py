import os
import json
from concurrent.futures import ThreadPoolExecutor
from constants import Constants
from util import  ParseDocuments, ParseDocument
import ast

# Read all the documents from the directory and preprocess the documents
def readDocuments() : 
    documents = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(__readDocument, filename): filename for filename in os.listdir(Constants.DATA_PATH)}
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

def write(path, filename, data, replaceQuotes = True) :
    complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename
    with open(complete_filemane, 'a+') as f:
        str = json.dumps(data, separators=(',', ':'), indent=None)
        if replaceQuotes:
            str = str.replace('"', '')
        f.write(str)

def readCatalog(path) :
    try :
        complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + Constants.CATALOG_FILE_NAME + '.json'
        with open(complete_filemane, 'r') as f:
            data = f.read()
        return json.loads(data)
    except FileNotFoundError:
        return {}

def currentOffset(path, filename):
    try:
        complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename
        with open(complete_filemane, 'r') as f:
            f.seek(0, os.SEEK_END)
            return f.tell()
    except FileNotFoundError:
        return 0

def seekFile(path, filename, offset, length):
    with open(Constants.OUTPUT_PATH + '/' + path + '/'  + filename, 'r') as f:
        f.seek(offset)
        return ast.literal_eval(f.read(length))
    

def __readDocument(filename) :
    with open(os.path.join(Constants.DATA_PATH, filename), 'rb') as f:
        content = f.read().decode("iso-8859-1")
    return content