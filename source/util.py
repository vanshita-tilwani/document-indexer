from constants import Constants
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import re
from nltk.stem import PorterStemmer

def ProcessQueries(queries, stopwords):
    processedQueries = {}
    for query in queries:
        processedQuery = query.split("->")
        id = int(processedQuery[0].strip())
        text = __preprocessText(processedQuery[1].strip(), stopwords)
        processedQueries[id] = text
    return processedQueries

# Preprocess the documents i,e tokenize and remove stopwords
def PreprocessDocuments(document_mapping, documents, stopwords):
    # Split documents into batches of size batch_size
    total_batches = [dict(list(documents.items())[i:i+Constants.BATCH_SIZE]) for i in range(0, len(documents), Constants.BATCH_SIZE)]

    # Preprocess documents in a batch
    processedDocuments = {}
    with ThreadPoolExecutor() as executor:
        # Process each batch in a separate thread
        futures = {executor.submit(__processBatch, batch, stopwords): batch for batch in total_batches}
        for future in as_completed(futures):
            try:
                processedBatch = future.result()
                processedDocuments.update(processedBatch)
            except Exception as exc:
                print(f"Batch processing generated an exception: {exc}")

    __updateDocumentMapping(document_mapping, processedDocuments)

    return processedDocuments

def __updateDocumentMapping(document_mapping, processedDocuments):
    for key, value in processedDocuments.items():
        ttf = len(value)
        document_mapping[key]['size'] = ttf

def StemData(documents):
    stemmedDocuments = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(__stemDocument, docText): docID for docID, docText in documents.items()}
        for future in concurrent.futures.as_completed(futures):
            docID = futures[future]
            try:
                stemmedDocuments[docID] = future.result()
            except Exception as exc:
                print(f"Document {docID} generated an exception: {exc}")
    return stemmedDocuments

# Parses the document ID and text for the document
def ParseDocument(document):
    docID = __parseDocumentID(document)
    docText = __parseDocumentText(document)
    return docID, docText

# Parse all the documents from the given content
def ParseDocuments(content: str) :
    pattern = '(?s)(?<=<DOC>)(.*?)(?=</DOC>)'
    return re.findall(pattern, content)

def __processBatch(batch, stopwords):
        results = {}
        global document_mapping
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(__preprocessText, docText, stopwords): docID for docID, docText in batch.items()}
            for future in as_completed(futures):
                docID = futures[future]
                try:
                    results[docID] = future.result()
                except Exception as exc:
                    print(f"Document {docID} generated an exception: {exc}")
        return results

def TokenizeDocumentIds(documents):
    tokenizedDocuments = {}
    global document_mapping
    for index, document in enumerate(documents.items()):
        id = document[0]
        text = document[1]
        tokenizedDocuments[index+1] = text
        document_mapping[index+1] = {'id': id}
    return document_mapping, tokenizedDocuments


# Parse the document ID from the document
def __parseDocumentID(document: str) :
    pattern = '(?s)(?<=<DOCNO>)(.*?)(?=</DOCNO>)'
    return re.search(pattern, document).group().strip()

# Parse the document text from the document
def __parseDocumentText(document: str) :
    pattern = '(?s)(?<=<TEXT>)(.*?)(?=</TEXT>)'
    return " ".join(re.findall(pattern, document))

def __stemDocument(docText):
    stemmedText = __stem(docText)
    return stemmedText

# Preprocess the given document i,e tokenize and remove stopwords
def __preprocessText(docText, stopwords):
    docText = __tokenize(docText)
    docText = __removeStopWords(docText, stopwords)
    return docText

# Remove the stopwords from the given text
def __removeStopWords(docText, stopwords):
    for s in stopwords:
        while s in docText:
            docText.remove(s)
    return docText

def __stem(words):
    stemmedWords = []
    for i in range(len(words)):
        stemmedWords.append(stemmer.stem(words[i]))
    return stemmedWords

# Tokenize the given text
def __tokenize(text) :
    # Replace comma and apostrophe with space
    content = __replaceCommaAndAprotophe(text, ' ')
    # Lowercase the text
    content = content.lower()
    # Split the text into words
    tokens = content.split()
    updatedTokens : list = []
    # Handle special cases
    for token in tokens:
        updatedToken = token
        if not __isFloat(token) :
            updatedToken = token.translate({ ord(i): None for i in Constants.PUNCTUATIONS})
        updatedTokens.append(updatedToken)
    return updatedTokens

# Check if the given string is a float
def __isFloat(string):
  try:
    return float(string) and '.' in string
  except ValueError:
    return False

# Replace comma and apostrophe with the given character 
def __replaceCommaAndAprotophe(text, character) :
    text = text.replace(Constants.COMMA, character)
    text = text.replace(Constants.APOSTROPHE, character)
    return text

stemmer = PorterStemmer()
document_mapping = {}