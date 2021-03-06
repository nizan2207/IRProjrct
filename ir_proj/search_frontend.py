from flask import Flask, request, jsonify
import sys
import re
from collections import Counter, OrderedDict, defaultdict
import itertools
from itertools import islice, count, groupby
import os
import operator
from operator import itemgetter
from pathlib import Path
import pickle
from contextlib import closing
import numpy as np
import pandas as pd
import math
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from google.cloud import storage
import traceback
import hashlib

# tokenize either with just english or all stopwords
def tokenize(text, stopwords_set='english'):
    """
    This function aims in tokenize a text into a list of tokens. Moreover, it filter stopwords.

    Parameters:
    -----------
    text: string , represting the text to tokenize.
    stopwords: english for english stopwords, all to include corpus stopwords. default english

    Returns:
    -----------
    list of tokens (e.g., list of tokens).
    """
    assert stopwords_set == "all" or stopwords_set == "english"  # validate input for debugging

    # make stopwords set
    english_stopwords = frozenset(stopwords.words('english'))
    corpus_stopwords = ['category', 'references', 'also', 'links', 'extenal', 'see', 'thumb']
    stops = None
    if stopwords_set == "all":
        stops = english_stopwords.union(corpus_stopwords)
    elif stopwords_set == "english":
        stops = english_stopwords

    # tokenize
    RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)
    list_of_tokens = [token.group() for token in RE_WORD.finditer(text.lower()) if
                      token.group() not in stops]
    return list_of_tokens


def _hash(s):
    return hashlib.blake2b(bytes(s, encoding='utf8'), digest_size=5).hexdigest()


english_stopwords = frozenset(stopwords.words('english'))
corpus_stopwords = ['category', 'references', 'also', 'links', 'extenal', 'see', 'thumb']
RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)

all_stopwords = english_stopwords.union(corpus_stopwords)

def word_count(text, id):
  ''' Count the frequency of each word in `text` (tf) that is not included in 
  `all_stopwords` and return entries that will go into our posting lists. 
  Parameters:
  -----------
    text: str
      Text of one document
    id: int
      Document id
  Returns:
  --------
    List of tuples
      A list of (token, (doc_id, tf)) pairs 
      for example: [("Anarchism", (12, 5)), ...]
  '''
  tokens = [token.group() for token in RE_WORD.finditer(text.lower())]
  # YOUR CODE HERE
  word_dict = dict()
  for word in tokens:
    if word in all_stopwords:
      continue
    if word not in word_dict.keys():
      word_dict[word] = [id, 0]
    word_dict[word][1] += 1
  res = [(entry[0], tuple(entry[1])) for entry in word_dict.items()]
  return res

NUM_BUCKETS = 124
def token2bucket_id(token):
  return int(_hash(token),16) % NUM_BUCKETS

"""# Inverted Index"""

BLOCK_SIZE = 1999998
class MultiFileWriter:
    """ Sequential binary writer to multiple files of up to BLOCK_SIZE each. """

    def __init__(self, base_dir, name):
        self._base_dir = Path(base_dir)
        self._name = name
        self._file_gen = (open(self._base_dir / f'{name}_{i:03}.bin', 'wb')
                          for i in itertools.count())
        self._f = next(self._file_gen)

    def write(self, b):
        locs = []
        while len(b) > 0:
            pos = self._f.tell()
            remaining = BLOCK_SIZE - pos
            # if the current file is full, close and open a new one.
            if remaining == 0:
                self._f.close()
                self._f = next(self._file_gen)
                pos, remaining = 0, BLOCK_SIZE
            self._f.write(b[:remaining])
            locs.append((self._f.name, pos))
            b = b[remaining:]
        return locs

    def close(self):
        self._f.close()

# reading and saving the titkle id dict so we can connect id to their title
id_title_pickle = "/content/drive/MyDrive/id_title_dict.pkl"
with open(id_title_pickle, 'rb') as f:
  id_title_dict = dict(pickle.loads(f.read()))

"""### Functions from previous Assignments"""
def generate_document_tfidf_matrix(query_to_search, index, words, pls):
    """
    Generate a DataFrame `D` of tfidf scores for a given query.
    Rows will be the documents candidates for a given query
    Columns will be the unique terms in the index.
    The value for a given document and term will be its tfidf score.

    Parameters:
    -----------
    query_to_search: list of tokens (str). This list will be preprocessed in advance (e.g., lower case, filtering stopwords, etc.').
                     Example: 'Hello, I love information retrival' --->  ['hello','love','information','retrieval']

    index:           inverted index loaded from the corresponding files.

    words,pls: generator for working with posting.
    Returns:
    -----------
    DataFrame of tfidf scores.
    """

    total_vocab_size = len(index.term_total)
    candidates_scores = get_candidate_documents_and_scores(query_to_search, index, words,pls)  # We do not need to utilize all document. Only the docuemnts which have corrspoinding terms with the query.

    unique_candidates = np.unique([doc_id for doc_id, freq in candidates_scores.keys()])
    D = np.zeros((len(unique_candidates), total_vocab_size))
    D = pd.DataFrame(D)
    

    D.index = unique_candidates
    D.columns = index.term_total.keys()

    for key in candidates_scores:
        tfidf = candidates_scores[key]
        doc_id, term = key
        D.loc[doc_id][term] = tfidf

    return D

def generate_query_tfidf_vector(query_to_search, index):
    """
    Generate a vector representing the query. Each entry within this vector represents a tfidf score.
    The terms representing the query will be the unique terms in the index.

    We will use tfidf on the query as well.
    For calculation of IDF, use log with base 10.
    tf will be normalized based on the length of the query.

    Parameters:
    -----------
    query_to_search: list of tokens (str). This list will be preprocessed in advance (e.g., lower case, filtering stopwords, etc.').
                     Example: 'Hello, I love information retrival' --->  ['hello','love','information','retrieval']

    index:           inverted index loaded from the corresponding files.

    Returns:
    -----------
    vectorized query with tfidf scores
    """
    epsilon = .0000001
    total_vocab_size = len(index.term_total)
    Q = np.zeros((total_vocab_size))
    term_vector = list(index.term_total.keys())
    counter = Counter(query_to_search)
    for token in np.unique(query_to_search):
        if token in index.term_total.keys():  # avoid terms that do not appear in the index.
            tf = counter[token] / len(query_to_search)  # term frequency divded by the length of the query
            df = index.df[token]
            idf = math.log((len(DL)) / (df + epsilon), 10)  # smoothing
            try:
                ind = term_vector.index(token)
                Q[ind] = tf * idf
            except:
                pass
    return Q
  
def generate_query_tfidf_vector1(query_to_search, words, DL, p_lst):
    """
    Generate a vector representing the query. Each entry within this vector represents a tfidf score.
    The terms representing the query will be the unique terms in the index.

    We will use tfidf on the query as well.
    For calculation of IDF, use log with base 10.
    tf will be normalized based on the length of the query.

    Parameters:
    -----------
    query_to_search: list of tokens (str). This list will be preprocessed in advance (e.g., lower case, filtering stopwords, etc.').
                     Example: 'Hello, I love information retrival' --->  ['hello','love','information','retrieval']

    index:           inverted index loaded from the corresponding files.

    Returns:
    -----------
    vectorized query with tfidf scores
    """

    epsilon = .0000001
    total_vocab_size = len(words) ## equal len(words)
    Q = np.zeros((total_vocab_size))
    term_vector = words ## equal words
    counter = Counter(query_to_search)
    for token in np.unique(query_to_search):
        if token in words:  # avoid terms that do not appear in the index.
            tf = counter[token] / len(query_to_search)  # term frequency divded by the length of the query
            ind = words.index(token)
            df = len(p_lst[ind])
            idf = math.log(DL / (df + epsilon), 10)
            
            Q[ind] = tf * idf
            
    return Q

def get_candidate_documents_and_scores(query_to_search, index, words, pls):
    """
    Generate a dictionary representing a pool of candidate documents for a given query. This function will go through every token in query_to_search
    and fetch the corresponding information (e.g., term frequency, document frequency, etc.') needed to calculate TF-IDF from the posting list.
    Then it will populate the dictionary 'candidates.'
    For calculation of IDF, use log with base 10.
    tf will be normalized based on the length of the document.

    Parameters:
    -----------
    query_to_search: list of tokens (str). This list will be preprocessed in advance (e.g., lower case, filtering stopwords, etc.').
                     Example: 'Hello, I love information retrival' --->  ['hello','love','information','retrieval']

    index:           inverted index loaded from the corresponding files.

    words,pls: generator for working with posting.
    Returns:
    -----------
    dictionary of candidates. In the following format:
                                                               key: pair (doc_id,term)
                                                               value: tfidf score.
    """
    candidates = {}
    N = len(DL)

    for term in np.unique(query_to_search):
        if term in words:
            list_of_doc = pls[words.index(term)]
            #nizan###changed str to int in doc
            normlized_tfidf = [(doc_id, (freq / DL[int(doc_id)]) * math.log(N / index.df[term], 10)) for doc_id, freq in
                               list_of_doc]

            for doc_id, tfidf in normlized_tfidf:
                candidates[(doc_id, term)] = candidates.get((doc_id, term), 0) + tfidf

    return candidates

def get_top_n(sim_dict,N=3):
    """ 
    Sort and return the highest N documents according to the cosine similarity score.
    Generate a dictionary of cosine similarity scores 
   
    Parameters:
    -----------
    sim_dict: a dictionary of similarity score as follows:
                                                                key: document id (e.g., doc_id)
                                                                value: similarity score. We keep up to 5 digits after the decimal point. (e.g., round(score,5))

    N: Integer (how many documents to retrieve). By default N = 3
    
    Returns:
    -----------
    a ranked list of pairs (doc_id, score) in the length of N.
    """
    #print(sim_dict.items())
    if len(sim_dict.items()) < N:
      N = len(sim_dict.items())
    #print(N)
    #print(type(sim_dict))
    #return sorted([(doc_id,round(score,5)) for doc_id, score in sim_dict.items()], key = lambda x: x[1],reverse=True)[:N]
    new_dict = dict(sorted(sim_dict.items(), key=lambda item: item[1]))
    return list(new_dict.items())[:N]

"""# Search frontend"""

def top_N_documents(df, N):
    """
    This function sort and filter the top N docuemnts (by score) for each query.

    Parameters
    ----------
    df: DataFrame (queries as rows, documents as columns)
    N: Integer (how many document to retrieve for each query)

    Returns:
    ----------
    top_N: dictionary is the following stracture:
          key - query id.
          value - sorted (according to score) list of pairs lengh of N. Eac pair within the list provide the following information (doc id, score)
    """
    # YOUR CODE HERE
    keys = list(df.T.columns)
    top_dict = dict()
    for key in keys:
        top_dict[key] = []
        for doc_id in range(len(df.columns)):
            top_dict[key].append((doc_id, df[doc_id][key]))
        top_dict[key].sort(key=lambda x: x[1], reverse=True)
        top_dict[key] = top_dict[key][:N]
    return top_dict

BLOCK_SIZE = 1999998


class MultiFileWriter:
    """ Sequential binary writer to multiple files of up to BLOCK_SIZE each. """

    def __init__(self, base_dir, name):
        self._base_dir = Path(base_dir)
        self._name = name
        self._file_gen = (open(self._base_dir / f'{name}_{i:03}.bin', 'wb')
                          for i in itertools.count())
        self._f = next(self._file_gen)

    def write(self, b):
        locs = []
        while len(b) > 0:
            pos = self._f.tell()
            remaining = BLOCK_SIZE - pos
            # if the current file is full, close and open a new one.
            if remaining == 0:
                self._f.close()
                self._f = next(self._file_gen)
                pos, remaining = 0, BLOCK_SIZE
            self._f.write(b[:remaining])
            locs.append((self._f.name, pos))
            b = b[remaining:]
        return locs

    def close(self):
        self._f.close()


class MultiFileReader:
    """ Sequential binary reader of multiple files of up to BLOCK_SIZE each. """

    def __init__(self):
        self._open_files = {}

    def read(self, locs, n_bytes, bin_path='body_bins'):
        b = []
        for f_name, offset in locs:
            if f_name not in self._open_files:
              path = '/content/drive/MyDrive/' + bin_path + '/'
              f_name = path + f_name
              self._open_files[f_name] = open(f_name, 'rb')
            f = self._open_files[f_name]
            f.seek(offset)
            #nizan
            #n_read = min(n_bytes, BLOCK_SIZE - offset)
            #implement min function in myself
            if n_bytes < BLOCK_SIZE - offset:
              n_read = n_bytes
            else:
              n_read = BLOCK_SIZE - offset
            b.append(f.read(n_read))
            n_bytes -= n_read
        return b''.join(b)

    def close(self):
        for f in self._open_files.values():
            f.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

TUPLE_SIZE = 6       # We're going to pack the doc_id and tf values in this 
                     # many bytes.
TF_MASK = 2 ** 16 - 1 # Masking the 16 low bits of an integer

DL = {}  # We're going to update and calculate this after each document. This will be usefull for the calculation of AVGDL (utilized in BM25)
def read_posting_list(index, w, name):
    # dynamically find the bin's folder based on its name
    bin_folder = ""
    if name == "body_index" or name == "body_index.pkl":
      bin_folder = "body_bins"
    elif name == "title_index":  
      bin_folder = "bins"
    elif name == "anchor_index":
      bin_folder = "posting_anchor"
    
    with closing(MultiFileReader()) as reader:
        locs = index.posting_locs[w]
        try:
            b = reader.read(locs, index.df[w] * TUPLE_SIZE, bin_path=bin_folder)
        except Exception as e:
            return []

        posting_list = []
        for i in range(index.df[w]):
            doc_id = int.from_bytes(b[i * TUPLE_SIZE:i * TUPLE_SIZE + 4], 'big')
            tf = int.from_bytes(b[i * TUPLE_SIZE + 4:(i + 1) * TUPLE_SIZE], 'big')
            posting_list.append((doc_id, tf))
    return posting_list

class InvertedIndex:  
  def __init__(self, docs={}):
    """ Initializes the inverted index and add documents to it (if provided).
    Parameters:
    -----------
      docs: dict mapping doc_id to list of tokens
    """
    # stores document frequency per term
    self.df = Counter()
    # stores total frequency per term
    self.term_total = Counter()
    # stores posting list per term while building the index (internally), 
    # otherwise too big to store in memory.
    self._posting_list = defaultdict(list)
    # mapping a term to posting file locations, which is a list of 
    # (file_name, offset) pairs. Since posting lists are big we are going to
    # write them to disk and just save their location in this list. We are 
    # using the MultiFileWriter helper class to write fixed-size files and store
    # for each term/posting list its list of locations. The offset represents 
    # the number of bytes from the beginning of the file where the posting list
    # starts. 
    self.posting_locs = defaultdict(list)
    
    for doc_id, tokens in docs.items(): 
      self.add_doc(doc_id, tokens)
  
  def posting_lists_iter(self):
    """ A generator that reads one posting list from disk and yields 
        a (word:str, [(doc_id:int, tf:int), ...]) tuple.
    """
    with closing(MultiFileReader()) as reader:
      for w, locs in self.posting_locs.items():
        # read a certain number of bytes into variable b
        b = reader.read(locs, self.df[w] * TUPLE_SIZE)
        posting_list = []
        # convert the bytes read into `b` to a proper posting list.
        
        for i in range(self.df[w]):
          doc_id = int.from_bytes(b[i*TUPLE_SIZE:i*TUPLE_SIZE+4], 'big')
          tf = int.from_bytes(b[i*TUPLE_SIZE+4:(i+1)*TUPLE_SIZE], 'big')
          posting_list.append((doc_id, tf))
        
        yield w, posting_list

  def add_doc(self, doc_id, tokens):
    """ Adds a document to the index with a given `doc_id` and tokens. It counts
        the tf of tokens, then update the index (in memory, no storage 
        side-effects).
    """
    DL[(doc_id)] = DL.get(doc_id,0) + (len(tokens))
    w2cnt = Counter(tokens)
    self.term_total.update(w2cnt)
    
    #max_value = max(w2cnt.items(), key=operator.itemgetter(1))[1]
    # There was a bug with the above line - max was overwritten by something mysterious.
    # Its easier to rewrite it than find the bug, so here you go:
    max_value = 0
    max_tup = None
    for tup in w2cnt.items():
      if tup[1] > max_value:
        max_value = tup[1]
        max_tup = tup



    # frequencies = {key: value/max_value for key, value in frequencies.items()}
    for w, cnt in w2cnt.items():        
        self.df[w] = self.df.get(w, 0) + 1                
        self._posting_list[w].append((doc_id, cnt))


  def write(self, base_dir, name):
    """ Write the in-memory index to disk and populate the `posting_locs`
        variables with information about file location and offset of posting
        lists. Results in at least two files: 
        (1) posting files `name`XXX.bin containing the posting lists.
        (2) `name`.pkl containing the global term stats (e.g. df).
    """
    #### POSTINGS ####
    self.posting_locs = defaultdict(list)
    with closing(MultiFileWriter(base_dir, name)) as writer:
      # iterate over posting lists in lexicographic order
      for w in sorted(self._posting_list.keys()):
        self._write_a_posting_list(w, writer, sort=True)
    #### GLOBAL DICTIONARIES ####
    self._write_globals(base_dir, name)

  def _write_globals(self, base_dir, name):
    with open(Path(base_dir) / f'{name}.pkl', 'wb') as f:
      pickle.dump(self, f)

  def _write_a_posting_list(self, w, writer, sort=False):
    # sort the posting list by doc_id
    pl = self._posting_list[w]
    if sort:
      pl = sorted(pl, key=itemgetter(0))
    # convert to bytes    
    b = b''.join([(int(doc_id) << 16 | (tf & TF_MASK)).to_bytes(TUPLE_SIZE, 'big')
                  for doc_id, tf in pl])
    # write to file(s)
    locs = writer.write(b)
    # save file locations to index
    self.posting_locs[w].extend(locs)

  def __getstate__(self):
    """ Modify how the object is pickled by removing the internal posting lists
        from the object's state dictionary. 
    """
    state = self.__dict__.copy()
    del state['_posting_list']
    return state

  @staticmethod
  def read_index(base_dir, name):
    with open(Path(base_dir) / f'{name}.pkl', 'rb') as f:
      return pickle.load(f)

  @staticmethod
  def delete_index(base_dir, name):
    path_globals = Path(base_dir) / f'{name}.pkl'
    path_globals.unlink()
    for p in Path(base_dir).rglob(f'{name}_*.bin'):
      p.unlink()

def get_top_100_ids_after_cossim(doc_tfidf_dict,query_tfidf_dict,query):
    # make an empty dict of similarity values
    doc_sim_vals = dict()
    for doc_id, words_tfidf in doc_tfidf_dict.items():
        top, doc_sim_vals[doc_id]  = 0, 0

        # calculate the numerator
        for word, word_tfidf in words_tfidf:
            top += word_tfidf * query_tfidf_dict[word]
        
        bottom = 315000 * len(query)  # assume the average doc length and multiply by query length.
        if bottom != 0:
            doc_sim_vals[doc_id] = top / bottom

    # return the documents ordered by cosine similarity
    top_n = [doc_id for doc_id, _ in sorted(doc_sim_vals.items(), key=lambda item: item[1], reverse=True)][:100]
    return top_n

def get_top_n_ids_after_cossim(doc_tfidf_dict,query_tfidf_dict,query):
    # make an empty dict of similarity values
    doc_sim_vals = dict()
    for doc_id, words_tfidf in doc_tfidf_dict.items():
        top, doc_sim_vals[doc_id] = 0, 0  # initialize the dot product and the value for doc_id to 0

        # calculate the dot product. (top part of the function)
        for word, word_tfidf in words_tfidf:
            top += word_tfidf * query_tfidf_dict[word]

        bottom = 315000 * len(query)  # assume the average doc length and multiply by query length.
        if bottom != 0:
            doc_sim_vals[doc_id] = top / bottom  # calculate relative cosine sim

    # return the documents ordered by cosine similarity
    top_n = [doc_id for doc_id, _ in sorted(doc_sim_vals.items(), key=lambda item: item[1], reverse=True)]
    return top_n
    

class MyFlaskApp(Flask):
    def run(self, host=None, port=None, debug=False, **options):
      #load index.pkl into variable named inverted
      # self.index = InvertedIndex.read_index()
      with open("/content/drive/MyDrive/bins/title_index.pkl", 'rb') as f:
        inverted = pickle.loads(f.read())
        self.title_index = inverted
        

      with open("/content/drive/MyDrive/body_bins/body_index.pkl", 'rb') as f:
        inverted = pickle.loads(f.read())
        self.body_index = inverted
      

      with open("/content/drive/MyDrive/posting_anchor/anchor_index.pkl", 'rb') as f:
        inverted = pickle.loads(f.read())
        self.anchor_index = inverted
      """
      # PageRank
      with open('doc_PR_dict.pkl', 'rb') as f:
          self.doc_PR_dict = pickle.loads(f.open())
      """
      super(MyFlaskApp, self).run(host=host, port=port, debug=debug, **options)

app = MyFlaskApp(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False


@app.route("/search")
def search():
    ''' Returns up to a 100 of your best search results for the query. This is 
        the place to put forward your best search engine, and you are free to
        implement the retrieval whoever you'd like within the bound of the 
        project requirements (efficiency, quality, etc.). That means it is up to
        you to decide on whether to use stemming, remove stopwords, use 
        PageRank, query expansion, etc.

        To issue a query navigate to a URL like:
         http://YOUR_SERVER_DOMAIN/search?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of up to 100 search results, ordered from best to worst where each 
        element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')

    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    bod = body_search_100(app.body_index, query)
    title = title_search(app.title_index, query)
    m = merge(bod, title, 20)
    print(m)
    return jsonify(m)

@app.route("/search_body")
def search_body():
    ''' Returns up to a 100 search results for the query using TFIDF AND COSINE
        SIMI.LARITY OF THE BODY OF ARTICLES ONLY DO NOT use stemming. DO USE the
        staff-provided tokenizer from Assignment 3 (GCP part) to do the
        tokenization and remove stopwords. 

        To issue a query navigate to a URL like:
         http://YOUR_SERVER_DOMAIN/search_body?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of up to 100 search results, ordered from best to worst where each 
        element is a tuple (wiki_id, title).
    '''

    # NOTE May need token2bucket. Our tokenizer isnt from a3 gcp.
    res = []
    request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    print("Search_body")
    res = body_search_100(app.body_index, tokenize(query, stopwords_set="all"))
    # END SOLUTION
    return jsonify(res)

# Many of these functions use numpy and pandas:

@app.route("/search_title")
def search_title():
    ''' Returns ALL (not just top 100) search results that contain A QUERY WORD 
        IN THE TITLE of articles, ordered in descending order of the NUMBER OF 
        QUERY WORDS that appear in the title. For example, a document with a 
        title that matches two of the query words will be ranked before a 
        document with a title that matches only one query term. 

        Test this by navigating to the a URL like:
         http://YOUR_SERVER_DOMAIN/search_title?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ALL (not just top 100) search results, ordered from best to 
        worst where each element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    # setup - tokenize query, rename index
    tit_ind = app.title_index
    query = tokenize(query, stopwords_set='all')

    doc_counts = dict()
    for word in query:
        word_postings = read_posting_list(tit_ind, word, "title_index")
        for id, tf in word_postings:
            if id not in doc_counts.keys():
                doc_counts[id] = 0
            doc_counts[id] += tf
    res = sorted(list(doc_counts.items()), key=lambda item: item[1], reverse=True)
    res = [(item[0], id_title_dict[item[0]]) for item in res]

    # END SOLUTION
    return jsonify(res)

@app.route("/search_anchor")
def search_anchor():
    ''' Returns ALL (not just top 100) search results that contain A QUERY WORD 
        IN THE ANCHOR TEXT of articles, ordered in descending order of the 
        NUMBER OF QUERY WORDS that appear in anchor text linking to the page. 
        For example, a document with a anchor text that matches two of the 
        query words will be ranked before a document with anchor text that 
        matches only one query term. 

        Test this by navigating to the a URL like:
         http://YOUR_SERVER_DOMAIN/search_anchor?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ALL (not just top 100) search results, ordered from best to 
        worst where each element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    res = anchor_search(app.anchor_index, query)
    # END SOLUTION
    return jsonify(res)

@app.route("/get_pagerank", methods=['POST'])
def get_pagerank():
    ''' Returns PageRank values for a list of provided wiki article IDs. 

        Test this by issuing a POST request to a URL like:
          http://YOUR_SERVER_DOMAIN/get_pagerank
        with a json payload of the list of article ids. In python do:
          import requests
          requests.post('http://YOUR_SERVER_DOMAIN/get_pagerank', json=[1,5,8])
        As before YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of floats:
          list of PageRank scores that correrspond to the provided article IDs.
    '''

    """
    res = []
    wiki_ids = request.get_json()
    if len(wiki_ids) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    for id in wiki_ids:
        id_val = 0
        try:
            id_val = self.doc_PR_dict[str(id)]
        except:
            pass
        res.append(value)
    """

    # END SOLUTION
    return jsonify(res)

@app.route("/get_pageview", methods=['POST'])
def get_pageview():
    ''' Returns the number of page views that each of the provide wiki articles
        had in August 2021.

        Test this by issuing a POST request to a URL like:
          http://YOUR_SERVER_DOMAIN/get_pageview
        with a json payload of the list of article ids. In python do:
          import requests
          requests.post('http://YOUR_SERVER_DOMAIN/get_pageview', json=[1,5,8])
        As before YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ints:
          list of page view numbers from August 2021 that correrspond to the 
          provided list article IDs.
    '''
    res = []
    wiki_ids = request.get_json()
    if len(wiki_ids) == 0:
      return jsonify(res)
    # BEGIN SOLUTION

    # END SOLUTION
    return jsonify(res)

def title_search(index, query):
    # setup - tokenize query, rename index
    tit_ind = app.title_index
    if type(query) is not list:
        query = tokenize(query, stopwords_set='all') 

    doc_counts = dict()
    for word in query:
        word_postings = read_posting_list(tit_ind, word, "title_index")
        print("word_postings ", word_postings)
        for id, tf in word_postings:
            if id not in doc_counts.keys():
                doc_counts[id] = 0
            doc_counts[id] += tf
    res = sorted(list(doc_counts.items()), key=lambda item: item[1], reverse=True)
    res = [(item[0], id_title_dict[item[0]]) for item in res]
    return res

def body_search_100(index, query):
    if len(query) == 0:
        return jsonify(res)
    if type(query) is str:
        query = tokenize(query, stopwords_set='all')
    # BEGIN SOLUTION
    doc_tfidf_dict = {}  # this will save us a dict in the shape of key as the doc id and thev alue will be his score
    query_tfidf_dict = {}  # this will save us a dict in the shape of key as thwe word in the query and its value will be the word tfidf score

    # Save postings lists to memory, calculate dfs
    word_postings = dict()
    word_df = dict()
    for Qword in query:  # going through all of the words in the query and calculation their tfidf scores so we will save time in our calculations
        try:
            # Here we try to calculate fast_cosine from chapter 7, while normalizing by length. Our calc_DL function
            # didn't run independently from Flask, so we replaced it with what seemed to be a reasonable average.
            word_postings[Qword] = read_posting_list(index, Qword, "body_index.pkl")
            word_df[Qword] = len(word_postings[Qword])

            # calculate tfidf
            tf = query.count(Qword)
            #print(f"Word: {Qword}, word_df: {word_df[Qword]}")
            idf = math.log10(315000 / word_df[Qword])
            tfidf = tf * idf
            query_tfidf_dict[Qword] = tfidf
            for id, doc_tf in word_postings[Qword]:
                doc_tfidf = idf * doc_tf / 315000
                doc_tfidf_dict[id] = doc_tfidf_dict.get(id, [])
                doc_tfidf_dict[id].append((Qword, doc_tfidf))
        except Exception as e:
            print(e)
        
    if len(word_postings) == 0:
        return jsonify([("", "")])
    top_n = get_top_100_ids_after_cossim(doc_tfidf_dict, query_tfidf_dict,
                                         query)  # recieve the top 100 score based on the body index
    res = []
    for i in top_n:
        res.append((i, id_title_dict[i]))  # attaching the title to the doc based on the doc id
    return res

def anchor_search(index, query):
    # setup - tokenize query, rename index
    tit_ind = index
    if type(query) is not list:
        query = tokenize(query, stopwords_set='all')

    # count the anchor frequenices by going through the posting
    doc_counts = dict()
    for word in query:
        word_postings = read_posting_list(tit_ind, word, "anchor_index")
        for id, tf in word_postings:
            if id not in doc_counts.keys():
                doc_counts[id] = 0
            doc_counts[id] += tf
    res = sorted(list(doc_counts.items()), key=lambda item: item[1], reverse=True)

    # print the id, title for every item we have a title for
    res2 = []
    for item in res:
        try:
            res2.append((item[0], id_title_dict[item[0]]))
        except Exception as e:
            pass
    return res2

def calc_DL():
    DL = dict()
    counter = 0
    for word in inverted.df.keys():
        p_lst = read_posting_list(inverted, word, "body_index.pkl")
        for doc_id, tf in p_lst:
            if doc_id not in DL.keys():
                DL[doc_id] = 0
            DL[doc_id] += tf
            if counter % 20 == 0:
                print(f"Counter: {counter}, word: {word}")
    pickle.dump(DL)

# Merges 2 - 3 lists by taking the last num1 items in list 1 and replacing them with the first num1 items in list2.
# (start from 40)
def merge(res1, res2, num1, res3=None, num2=0):
    limit = min(40, len(res1))
    if num2 == 0:
        return res1[:-num1] + res2[:num1]
    if res3 != None:
        assert num1 + num2 < limit  # make sure that the list cant grow from the merge.
        return res1[:limit - num1 - num2] + res2[:num1] + res3[:num2 - 1]

if __name__ == '__main__':
    #run flask app
    app.run(host='0.0.0.0',port=8080,debug=False)
"""## Body Index"""


def id_title_tup(id):
  return

# Tests
# NOTE May need token2bucket. Our tokenizer isnt from a3 gcp.
