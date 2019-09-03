"""concrete class that performs TFIDF on lemmatized tokens with optional ngrams

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from nltk import ngrams
from primrose.base.search_engine import AbstractSearchEngine
from nltk import WordNetLemmatizer

class MinimalSearchEngine(AbstractSearchEngine):
    """simple TFIDF search engine"""

    def __init__(self, configuration, instance_name):
        """instantiate the search engine

        Args:
            configuration (Configuration): Configuration instance
            instance_name (str): name of instance

        """
        AbstractSearchEngine.__init__(self, configuration, instance_name)
        self.lemmatizer = WordNetLemmatizer()

    def tokenize(self, s, stopwords=[], add_ngrams=True):
        """ tokenize a string document, optimized for recipe names given default stopwords and other
            string cleanup operations

        Args:
            s (str): some document string
            stopwords (list): list of stopwords
            add_ngrams (bool): whehter to include ngrams on tokens

        Returns
            tokens (list): list of cleaned, standardized tokens from document

        """
        q = s.lower().strip()
        tokens = q.replace.split(" ")
        tokens = [w for w in tokens if w not in stopwords]
        tokens = [self.lemmatizer.lemmatize(w) for w in tokens]
        if add_ngrams:
            bigrams = list(ngrams(tokens, 2))
            strbigrams = ["_".join(t) for t in bigrams]
            tokens.extend(strbigrams)
        return tokens
