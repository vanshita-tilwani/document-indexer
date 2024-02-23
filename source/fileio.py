import os
import json
from concurrent.futures import ThreadPoolExecutor
from constants import Constants
from util import ParseDocuments, ParseDocument
import ast
import glob
import zlib

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

# Writing to the file as JSON
def write(path, filename, data, replaceQuotes = True) :
    complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename
    with open(complete_filemane, 'a+') as f:
        str = json.dumps(data, separators=(',', ':'), indent=None)
        if replaceQuotes:
            str = str.replace('"', '')
        f.write(str)

def writeToBinary(path, filename, data):
    complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename
    with open(complete_filemane, 'ab+') as f:
        final_string_data = json.dumps(data, separators=(',', ':'), indent=None).replace('"', '')
        bytes_data = zlib.compress(final_string_data.encode())
        f.write(bytes_data)

# Reading queries from the file
def readQueries() :
    with open(Constants.QUERY_PATH, 'r') as f:
        content = f.read()
        queries = content.split("\n")
    return queries

# Reading the file as JSON
def read(path, filename) :
    try :
        complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename + '.json'
        with open(complete_filemane, 'r') as f:
            data = f.read()
        return json.loads(data)
    except FileNotFoundError:
        return {}

# Returns the current offset of the file
def offset(path, filename):
    try:
        complete_filemane = Constants.OUTPUT_PATH + '/' + path + '/'  + filename
        with open(complete_filemane, 'r') as f:
            f.seek(0, os.SEEK_END)
            return f.tell()
    except FileNotFoundError:
        return 0

# Get data using the offset and length from the file
def seek(path, filename, offset, length):
    with open(Constants.OUTPUT_PATH + '/' + path + '/'  + filename, 'r') as f:
        f.seek(offset)
        return ast.literal_eval(f.read(length))

def seekBinary(path, filename, offset, length):
    with open(Constants.OUTPUT_PATH + '/' + path + '/'  + filename, 'rb') as f:
        f.seek(offset)
        data = f.read(length)
        string_data = zlib.decompress(data).decode()
        return ast.literal_eval(string_data)
    
# Writing query execution result to file
def WriteToResults(path, model, query, score, document_mapping):
    try:
        out = open(Constants.OUTPUT_PATH + '/' + path + '/' + model + '.txt', 'a')
        for rank, (document, score)in enumerate(score):
            out.write(str(query) + ' Q0 ' + str(document_mapping[document]['id']) + ' ' +str((rank+1)) + ' ' + str(score) + ' Exp\n')
        out.close()
    except Exception as exception:
        print(exception)

# Cleaning the index directory
def Cleanup(type) :
    catalog_files = Constants.OUTPUT_PATH + '/' + type + '/catalog*'
    inverted_index = Constants.OUTPUT_PATH + '/' + type + '/inverted_index*'
    files = glob.glob(catalog_files)
    for f in files:
        os.remove(f)
    files = glob.glob(inverted_index)
    for f in files:
        os.remove(f)

def __readDocument(filename) :
    with open(os.path.join(Constants.DATA_PATH, filename), 'rb') as f:
        content = f.read().decode("iso-8859-1")
    return content