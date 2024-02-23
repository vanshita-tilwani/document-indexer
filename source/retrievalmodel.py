from collections import OrderedDict
from constants import Constants
from indexer import TermVector
import math

class Model() :
    def __init__(self, index_type, index, documents):
        self.index = index
        self.index_type = index_type
        self.documents = documents
        self.corpus_size = self.__corpusSize()
        self.total_term_frequency = {}
        
    def SetModel(self, model):
        self.model = model
    
    def SetQuery(self, query):
        self.query = query

    def Execute(self):
        if (self.model == Constants.PROXIMITY_SEARCH):
            result = self.__executeProximitySearch()
        else:
            result = self.__executeQueryForModel()
        orderedResult = OrderedDict(sorted(result.items(), key=lambda x: x[1]))
        scores = list(orderedResult.items())[::-1][:1000]
        return scores
            
    def __executeProximitySearch(self) :
        self.SetModel(Constants.BM_25)
        scores = self.__executeQueryForModel()
        info = {}
        for word in self.query :
            term_vector = TermVector(self.index_type, self.index, word)
            # If term exists in corpus, i.e. it exits in any of the document
            for doc, pos in term_vector.items():
                if doc in info:
                    info[doc][word] = pos
                else:
                    info[doc] = {word : pos}
        
        for doc, words in info.items():
            for word, pos in words.items():
                tf = len(pos)
                curr_acc = self.__accscore(len(self.documents), word, words)
                prox_score = self.__proximityscore(len(self.documents), tf, curr_acc, self.documents[doc]['size'], self.__averageLength())
                scores[doc] += prox_score
        return scores
    
    def __executeQueryForModel(self):
        scores = OrderedDict()
        for word in self.query:
            term_vector = TermVector(self.index_type, self.index, word)
            for id in self.documents:
                if id in term_vector:
                    score = self.__score(word, term_vector, id, self.query.count(word))
                else:
                    score = self.__scoreForAbsentTerm(word, term_vector, id)
                if id not in scores:
                    scores[id] = 0
                scores[id] += score
        return scores

    def __score(self, word, term_vector, docId, q =  1):
        match self.model:
            case Constants.OKAPI_TF :
                return self.__okapiTF(term_vector, docId)
            case Constants.TF_IDF :
                return self.__tfIdf(term_vector, docId)
            case Constants.BM_25 :
                return self.__bm25(term_vector, docId, q)
            case Constants.LM_LAPLACE :
                return self.__lm_laplace(term_vector, docId)
            case Constants.LM_JELINEKMERCER :
                return self.__lm_jelinekmercer(word, term_vector, docId)
            
    
    def __scoreForAbsentTerm(self, word, term_vector, docId) :
        match self.model:
            case Constants.OKAPI_TF :
                return 0.0
            case Constants.TF_IDF :
                return 0.0
            case Constants.BM_25 :
                return 0.0
            case Constants.LM_LAPLACE :
                return -1000.0
            case Constants.LM_JELINEKMERCER :
                return self.__lm_jelinekmercer_absentterms(word, term_vector, docId)
        
    def __okapiTF(self, term_vector, docId):
        tf = len(term_vector[docId])
        length = self.documents[docId]['size']
        average_length = self.__averageLength()
        denominator = tf + 0.5 + (1.5 * (length/average_length))
        okapi_tf = tf/denominator
        return okapi_tf
    
    def __tfIdf(self,term_vector, docId):
        totalDocs = len(self.documents)
        df = len(term_vector)
        okapitf_wd = self.__okapiTF(term_vector, docId)
        tfidf = okapitf_wd * math.log(totalDocs/df)
        return tfidf
    
    def __bm25(self, term_vector, docId, qf = 1):
        tf = len(term_vector[docId])
        df = len(term_vector)
        length = self.documents[docId]['size']
        totalDocs = len(self.documents)
        averageLength = self.__averageLength()
        firstTerm = math.log((totalDocs + 0.5)/(df+0.5))
        secondTerm = (tf + Constants.BM25_K1 * tf)/(tf + Constants.BM25_K1*((1-Constants.BM25_B) + Constants.BM25_B * length/averageLength))
        thirdTerm = (qf + Constants.BM25_K2*qf)/(Constants.BM25_K2 + qf)
        bm25 = firstTerm * secondTerm * thirdTerm
        return bm25
    
    def __lm_laplace(self, term_vector, docId) :
        length = self.documents[docId]['size']
        tf = len(term_vector[docId])
        vocab_size = len(self.index)
        result = (tf + 1)/(length + vocab_size)
        return result
    
    def __lm_jelinekmercer(self, word, term_vector, docId) :
        tf = len(term_vector[docId])
        ttf = self.__totalTermFrequency(word, term_vector)
        length = self.documents[docId]['size']
        total_length = self.corpus_size
        vocab_size = len(self.index)
        foreground = Constants.CORPUS_PROB * (tf/vocab_size)
        background = (1 - Constants.CORPUS_PROB) * ((ttf - tf)/(total_length - length))
        score = foreground + background
        return math.log(score)
    
    def __lm_jelinekmercer_absentterms(self, word, term_vector, docId):
        length = self.documents[docId]['size']
        total_length = self.corpus_size
        # keeping it as avg if not present in corpus todo : might cause issue
        ttf = self.__averageLength() 
        if(len(term_vector) > 0):
            ttf = self.__totalTermFrequency(word, term_vector)
        score = (1-Constants.CORPUS_PROB)*(ttf/(total_length - length))
        return math.log(score)

    def __proximityscore(self, num_docs, tf, acc_t, len_d, avg_doc_len):
        ief = self.__iefscore(num_docs, tf)
        calc2 = (acc_t * (Constants.PROX_SEARCH_K1+1)) / (acc_t + (Constants.PROX_SEARCH_K1 * ((1-Constants.PROX_SEARCH_B) + (Constants.PROX_SEARCH_B * (len_d/avg_doc_len)))) ) 
        prox = min(ief, 1) * calc2
        return prox
    
    def __accscore(self, td, wrd, w_positions_dict) :
        curr_positions = w_positions_dict.get(wrd)
        acc = 0
        for w, p in w_positions_dict.items():
            if w == wrd:
                continue
        p1 = 0
        p2 = 0
        term_j_pos = p
        while (p1 < len(curr_positions) and p2 < len(term_j_pos)):
            window = abs(int(curr_positions[p1]) - int(term_j_pos[p2]))
            if window <= Constants.PROX_SEARCH_D:
                # log(total_docs - tf + 0.5 / tf + 1) / d^2
                tf = len(term_j_pos)
                temp_acc = self.__iefscore(td, tf) / Constants.PROX_SEARCH_D**2
                acc += temp_acc
            if int(curr_positions[p1]) < int(term_j_pos[p2]):
                p1 += 1
            else:
                p2 += 1
        while (p1 < len(curr_positions)):
            window = abs(int(curr_positions[p1]) - int(term_j_pos[p2-1])) # may need to do p2 -1 or else out of range
            if window <= Constants.PROX_SEARCH_D:
                tf = len(term_j_pos)
                temp_acc = self.__iefscore(td, tf) / Constants.PROX_SEARCH_D**2
                acc += temp_acc
            p1 += 1
        while (p2 < len(term_j_pos)):
            window = abs(int(curr_positions[p1-1]) - int(term_j_pos[p2]))
            if window <= Constants.PROX_SEARCH_D:
                tf = len(term_j_pos)
                temp_acc = self.__iefscore(td, tf) / Constants.PROX_SEARCH_D**2
                acc += temp_acc
            p2 += 1
        return acc
    
    def __iefscore(self, n, term_freq):
        return math.log( (n - (term_freq) + 1) / (term_freq + 0.5), 2)
    
    def __totalTermFrequency(self, word, term_vector):
        if word in self.total_term_frequency:
            return self.total_term_frequency[word]
        
        ttf = 0
        for docId in term_vector:
            ttf += len(term_vector[docId])
        self.total_term_frequency[word] = ttf
        return self.total_term_frequency[word]
    
    def __corpusSize(self):
        total_terms = 0
        for docId in self.documents:
            total_terms += self.documents[docId]['size']
        return total_terms
    
    def __averageLength(self):
        total_terms = self.corpus_size
        return total_terms/len(self.documents)
        