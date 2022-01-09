N = 6000000  # number of docs
Scores = dict()
for word in query:
	for doc_id, count in get_postings(word):
		if doc_id not in scores.keys():
			scores[doc_id] = 0
		scores[dic_id] += count
	# possible optimization: turn scores into a numpy array, keep doc_id-lengths as numpy array, and do a division (cant, different length vectors - unless scores is an array)
	# another optimization: manual doc_id-index mapping.

        # If scores >> 100, use a heap instead of list and sort.
        for doc in in scores.keys():
		scores[doc] = scores[doc]/lengths(doc)
	return sorted(list(scores.items()), key=lambda t: t[1], reverse=True)[:100]

        # I wasnt sure about heapify. More efficient to insert into a heap, but then you cant divide the sums by the length.
	#if len(scores.keys()) < 200:
        #        return sorted(list(scores.items()), key=lambda t: t[1], reverse=True)[:100]
        #else:
        #        heapq.
                
	
	
