#!/usr/bin/python3

import sys
import json

keywords = ["han", "hon", "den", "det", "denna", "denne", "hen"]

def read_json_obj(file):
    for line in file:
        # remove leading and trailing whitespace
        line = line.strip()
        # get the ones that are not empty
        if len(line): 
            json_obj = json.loads(line)
            yield json_obj
            # # remove the tweets that are not unique
            # if not 'retweeted_status' in json_obj: 
            #     text = json_obj['text'].strip()
            #     yield text.split()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_json_obj(sys.stdin)
    unique_tweets = 0
    # match with keywords
    for json_obj in data:
        if not 'retweeted_status' in json_obj: 
            print('%s\t%s' % ("UniqueTweet", 1))
            text = json_obj['text'].strip()
            words = text.split()
            for word in words:
                if word.lower() in keywords:
                    print('%s\t%s' % (word.lower(), 1))

    


if __name__ == "__main__":
    main()