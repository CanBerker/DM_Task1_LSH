import sys
from random import seed
from random import randrange
import ast


def mapper(key, value):
    dict_length = 8192

    key = key.split('*')
    num_per_band = int(key[0])
    a_hash = ast.literal_eval(key[1])
    b_hash = ast.literal_eval(key[2])

    input = value.split(" ")        # split page number and shingles
    page = input[0].split("_")      # split text and page id
    page_id = int(page[1])
    shingles = input[1:]
    shingles = [int(numeric_string) for numeric_string in shingles]     # cast string shingles to int
    shingles = sorted(shingles)


    def min_hash_fn(a, b, sig):
        hashes = [((a * x) + b) % dict_length for x in sig]
        return min(hashes)

    def get_min_hash_row(sig):
        hashes = [min_hash_fn(a, b, sig) for a, b in zip(a_hash, b_hash)]
        return hashes

    def get_band(l, n):
        for i in xrange(0, len(l), n):
            yield frozenset(l[i:i + n])


    signature = shingles
    min_hash_row = get_min_hash_row(signature)
    #print min_hash_row

    banded = get_band(min_hash_row, num_per_band)

    for band in banded:
        yield hash(band), page_id


def reducer(key, values):
    # key: key from mapper used to aggregate
    # values: list of all value for that key

    if len(values) > 1: # if there is a collision in the bucket
        values = sorted(values)
        for i in range(0,len(values)):
            for j in range(i+1, len(values)):
                #print values[i], values[j]
                yield values[i], values[j]

