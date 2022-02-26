#!/usr/bin/env python3

"""
Reference:
    https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
"""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word, and creates an iterator that returns consecutive keys and their group
    for current_word, group in groupby(data, itemgetter(0)):
        # print(group)
        try:
            total_count = sum(int(count) for current_word, count in group)
            print("%s%s%d" % (current_word, separator, total_count))
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()