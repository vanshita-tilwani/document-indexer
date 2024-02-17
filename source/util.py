from constants import Constants
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import re

# Parses the document ID and text for the document
def ParseDocument(document):
    docID = ParseDocumentID(document)
    docText = ParseDocumentText(document)
    return docID, docText

# Parse all the documents from the given content
def ParseDocuments(content: str) :
    pattern = '(?s)(?<=<DOC>)(.*?)(?=</DOC>)'
    return re.findall(pattern, content)

# Parse the document ID from the document
def ParseDocumentID(document: str) :
    pattern = '(?s)(?<=<DOCNO>)(.*?)(?=</DOCNO>)'
    return re.search(pattern, document).group().strip()

# Parse the document text from the document
def ParseDocumentText(document: str) :
    pattern = '(?s)(?<=<TEXT>)(.*?)(?=</TEXT>)'
    return " ".join(re.findall(pattern, document))

# Preprocess the documents i,e tokenize and remove stopwords
def PreprocessDocuments(documents, stopwords):
    tokenizedDocuments = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(PreprocessDocument, docID, docText, stopwords): docID for docID, docText in documents.items()}
        for future in concurrent.futures.as_completed(futures):
            docID = futures[future]
            try:
                tokenizedDocuments[docID] = future.result()
            except Exception as exc:
                print(f"Document {docID} generated an exception: {exc}")
    return tokenizedDocuments

# Preprocess the given document i,e tokenize and remove stopwords
def PreprocessDocument(docId, docText, stopwords):
    docText = Tokenize(docText)
    docText = RemoveStopWords(docText, stopwords)
    return docId, docText

# Remove the stopwords from the given text
def RemoveStopWords(docText, stopwords):
    for s in stopwords:
        while s in docText:
            docText.remove(s)
    return docText

# Tokenize the given text
def Tokenize(text) :
    # Replace comma and apostrophe with space
    content = ReplaceCommaAndAprotophe(text, ' ')
    # Lowercase the text
    content = content.lower()
    # Split the text into words
    tokens = content.split()
    updatedTokens : list = []
    # Handle special cases
    for token in tokens:
        updatedToken = token
        if not IsFloat(token) :
            updatedToken = token.translate({ ord(i): None for i in Constants.PUNCTUATIONS})
        updatedTokens.append(updatedToken)
    return updatedTokens

# Check if the given string is a float
def IsFloat(string):
  try:
    return float(string) and '.' in string
  except ValueError:
    return False

# Replace comma and apostrophe with the given character 
def ReplaceCommaAndAprotophe(text, character) :
    text = text.replace(Constants.COMMA, character)
    text = text.replace(Constants.APOSTROPHE, character)
    return text