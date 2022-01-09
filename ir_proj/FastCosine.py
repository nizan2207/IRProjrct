N = 6000000  # number of docs
Scores = dict()
for word in query:
	for doc_id, count in get_postings(word):
		if doc_id not in scores.keys():
			scores[doc_id] = 0
		scores[dic_id] += count
	# possible optimization: turn scores into a numpy array, keep doc_id-lengths as numpy array, and do a division
	for doc in in scores.keys():
		scores[doc] = scores[doc]