from constants import Constants
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import re
from nltk.stem import PorterStemmer

def GenerateInverseIndex(documents):
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

# Preprocess the documents i,e tokenize and remove stopwords
def PreprocessDocuments(documents, stopwords):

    # Preprocess documents in a batch
    def ProcessBatch(batch):
        results = {}
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(PreprocessDocument, docID, docText,stopwords): docID for docID, docText in batch.items()}
            for future in as_completed(futures):
                docID = futures[future]
                try:
                    results[docID] = future.result()
                except Exception as exc:
                    print(f"Document {docID} generated an exception: {exc}")
        return results

    # Split documents into batches of size batch_size
    total_batches = [dict(list(documents.items())[i:i+Constants.BATCH_SIZE]) for i in range(0, len(documents), Constants.BATCH_SIZE)]

    processedDocuments = {}
    with ThreadPoolExecutor() as executor:
        # Process each batch in a separate thread
        futures = {executor.submit(ProcessBatch, batch): batch for batch in total_batches}
        for future in as_completed(futures):
            try:
                processedBatch = future.result()
                processedDocuments.update(processedBatch)
            except Exception as exc:
                print(f"Batch processing generated an exception: {exc}")

    return processedDocuments

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

def StemDocuments(documents):
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(StemDocument, docID, docText): docID for docID, docText in documents.items()}
        for future in concurrent.futures.as_completed(futures):
            docID = futures[future]
            try:
                documents[docID] = future.result()
            except Exception as exc:
                print(f"Document {docID} generated an exception: {exc}")
    return documents

def StemDocument(docID, docText):
    docText = Stem(docText)
    return docID, docText

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

def Stem(words):
    for i in range(len(words)):
        words[i] = stemmer.stem(words[i])
    return words

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

stemmer = PorterStemmer()