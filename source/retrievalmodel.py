from collections import OrderedDict
from constants import Constants

class Model() :
    def __init__(self, model, index, query, document):
        self.index = index
        self.query = query
        self.document = document
        self.model = model

    def Execute(self):
        print('Executing the query on ' + self.model)
        
    
        
