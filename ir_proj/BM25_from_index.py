import math
from itertools import chain
import time
# When preprocessing the data have a dictionary of document length for each document saved in a variable called `DL`.
class BM25_from_index:
    """
    Best Match 25.
    ----------
    k1 : float, default 1.5

    b : float, default 0.75

    index: inverted index
    """

    def __init__(self ,index ,k1=1.5, b=0.75):
        self.b = b
        self.k1 = k1
        self.index = index
        self.N = 6348910
        self.AVGDL = sum(DL.values() ) /self.N  # need sum of all doc lengths - iterate and sum over them...
        self.words, self.pls = zip(*self.index.posting_lists_iter())

    def calc_idf(self, list_of_tokens):
        """
        This function calculate the idf values according to the BM25 idf formula for each term in the query.

        Parameters:
        -----------
        query: list of token representing the query. For example: ['look', 'blue', 'sky']

        Returns:
        -----------
        idf: dictionary of idf scores. As follows:
                                                    key: term
                                                    value: bm25 idf score
        """
        idf = {}
        for term in list_of_tokens:
            if term in self.index.df.keys():
                n_ti = self.index.df[term]
                idf[term] = math.log(1 + (self.N - n_ti + 0.5) / (n_ti + 0.5))
            else:
                pass
        return idf


    def search(self, queries ,N=3):
        """
        This function calculate the bm25 score for given query and document.
        We need to check only documents which are 'candidates' for a given query.
        This function return a dictionary of scores as the following:
                                                                    key: query_id
                                                                    value: a ranked list of pairs (doc_id, score) in the length of N.

        Parameters:
        -----------
        query: list of token representing the query. For example: ['look', 'blue', 'sky']
        doc_id: integer, document id.

        Returns:
        -----------
        score: float, bm25 score.
        """
        # YOUR CODE HERE
        query_cand_dict = dict()
        for q_id, query in queries.items():
            top_n_dict ={}
            self.idf = self.calc_idf(query)

            for doc_key in get_candidate_documents(query ,self.index ,self.words ,self.pls):
                top_n_dict[doc_key] = self._score(query ,doc_key)
            query_cand_dict[q_id] = get_top_n(top_n_dict ,N)

        return query_cand_dict

    def _score(self, query, doc_id):
        """
        This function calculate the bm25 score for given query and document.

        Parameters:
        -----------
        query: list of token representing the query. For example: ['look', 'blue', 'sky']
        doc_id: integer, document id.

        Returns:
        -----------
        score: float, bm25 score.
        """
        score = 0.0
        doc_len = DL[str(doc_id)]  # only need current doc length - the one you have.

        for term in query:
            if term in self.index.term_total.keys():
                term_frequencies = dict(self.pls[self.words.index(term)])
                if doc_id in term_frequencies.keys():
                    freq = term_frequencies[doc_id]
                    numerator = self.idf[term] * freq * (self.k1 + 1)
                    denominator = freq + self.k1 * (1 - self.b + self.b * doc_len / self.AVGDL)
                    score += (numerator / denominator)
        return score