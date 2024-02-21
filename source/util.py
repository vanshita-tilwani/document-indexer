from constants import Constants
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import re
from nltk.stem import PorterStemmer

def ProcessQueries(queries, stopwords):
    processedQueries = {}
    query_dict = {}
    for query in queries:
        processedQuery = query.split("->")
        query_dict[processedQuery[0]] = processedQuery[1]

    with ThreadPoolExecutor() as executor:
        # Process each query in a separate thread
        futures = {executor.submit(__preprocessText, queryText, stopwords): queryId for queryId, queryText in queries.items()}
        for future in as_completed(futures):
            queryID = futures[future]
            try:
                processedQueries[queryID] = future.result()
            except Exception as exc:
                print(f"Query processing generated an exception: {exc}")

    return processedQueries

# Preprocess the documents i,e tokenize and remove stopwords
def PreprocessDocuments(documents, stopwords):
    tokenizedDocuments = __tokenizeDocumentIds(documents)
    # Split documents into batches of size batch_size
    total_batches = [dict(list(tokenizedDocuments.items())[i:i+Constants.BATCH_SIZE]) for i in range(0, len(tokenizedDocuments), Constants.BATCH_SIZE)]

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

    return processedDocuments

def StemDocuments(documents):
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

def __tokenizeDocumentIds(documents):
    tokenizedDocuments = {}
    global document_mapping
    for index, document in enumerate(documents.items()):
        id = document[0]
        text = document[1]
        tokenizedDocuments[index] = text
        document_mapping[index] = id
    return tokenizedDocuments


# Parse the document ID from the document
def __parseDocumentID(document: str) :
    pattern = '(?s)(?<=<DOCNO>)(.*?)(?=</DOCNO>)'
    return re.search(pattern, document).group().strip()

# Parse the document text from the document
def __parseDocumentText(document: str) :
    pattern = '(?s)(?<=<TEXT>)(.*?)(?=</TEXT>)'
    return " ".join(re.findall(pattern, document))

def __stemDocument(docText):
    docText = __stem(docText)
    return docText

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
    for i in range(len(words)):
        words[i] = stemmer.stem(words[i])
    return words

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