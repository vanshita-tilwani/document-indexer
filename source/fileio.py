import os
import json
from concurrent.futures import ThreadPoolExecutor
from constants import Constants
from util import ParseDocuments, ParseDocument
import ast
import glob

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

def readQueries() :
    with open(Constants.QUERY_PATH, 'r') as f:
        content = f.read()
        queries = content.split("\n")
    return queries

def readFileAsJson(path, filename) :
    try :
        complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename + '.json'
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
    
def WriteToResults(path, model, query, score, document_mapping):
    try:
        out = open(Constants.OUTPUT_PATH + '/' + path + '/' + model + '.txt', 'a')
        for rank, (document, score)in enumerate(score):
            out.write(str(query) + ' Q0 ' + str(document_mapping[document]['id']) + ' ' +str((rank+1)) + ' ' + str(score) + ' Exp\n')
        out.close()
    except Exception as exception:
        print(exception)

def Cleanup(type) :
    path = Constants.OUTPUT_PATH + '/' + type + '/*'
    files = glob.glob(path)
    for f in files:
        os.remove(f)

def __readDocument(filename) :
    with open(os.path.join(Constants.DATA_PATH, filename), 'rb') as f:
        content = f.read().decode("iso-8859-1")
    return content