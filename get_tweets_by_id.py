from utility.util import split_list_to_fixed_length_lol, ez_split
from utility.util_local import read_input_csv, write_output_csv

from datetime import datetime, timedelta
import csv
import os
import sys
import argparse
import logging
import json

logging.getLogger().setLevel(logging.INFO)

try:
    import tweepy
except ImportError:
    sys.exit("~ Make sure you install tweepy. Run `pip install tweepy` and try this again ~")


argparser = argparse.ArgumentParser()
argparser.add_argument('-o', '--output_filename', nargs='?', default="! Resulting Tweets from IDs.csv", help="If you want an output filename other than the default")
argparser.add_argument('-f', '--filename', nargs='?', required=True, help="The input CSV")
argparser.add_argument('-c', '--column', nargs='?', required=True, help="The column in that CSV that the IDs are in")

args = argparser.parse_args()
args = vars(args) # convert to dict


###############################################################################


def get_account_tweets(tweet_id_list):
    auth_dict = json.loads(os.environ["TW_KEYS"])
    auth = tweepy.OAuthHandler(auth_dict["TW_CONSUMER_KEY"], auth_dict["TW_CONSUMER_SECRET"])
    auth.set_access_token(auth_dict["TW_ACCESS_KEY"], auth_dict["TW_ACCESS_SECRET"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    output_lod = []

    tweet_id_list = [ez_split(x, "status/", 1) for x in tweet_id_list]

    tweet_ids_lol = split_list_to_fixed_length_lol(tweet_id_list, 100)
    for tweet_id_slice in tweet_ids_lol:

        tweets = api.statuses_lookup(tweet_id_list)
        for tweet in tweets:
            # print((tweet.author._json).keys())
            output_lod.append({
                "tweet_url": f"twitter.com/{tweet.author.screen_name}/status/{tweet.id}",
                'text': tweet.text,
                'author_account': tweet.author.screen_name,
                'author_name': tweet.author.name,
                'author_description': tweet.author.description,
                'author_location': tweet.author.location,
                'author_url': tweet.author.url,
                'author_followers': tweet.author.followers_count,
                'author_following': tweet.author.friends_count,
                'author_statuses': tweet.author.statuses_count,
                'author_verified': tweet.author.verified,
                'created_at': tweet.created_at.strftime("%m/%d/%Y"),
                'time_zone': tweet.author.time_zone,
                'geo': tweet.geo,
                'id': tweet.id,
                'in_reply_to_screen_name': tweet.in_reply_to_screen_name,
                'in_reply_to_status_id': tweet.in_reply_to_status_id,
                'in_reply_to_user_id': tweet.in_reply_to_user_id,
                'is_quote_status': tweet.is_quote_status,
                'lang': tweet.lang,
                'likes': tweet.favorite_count,
                'retweets': tweet.retweet_count,
                'source': tweet.source,
            })

    return output_lod


if __name__ == "__main__":
    result_lod = []

    output_filename = args.pop("output_filename") + ".csv" if ".csv" not in args.get("output_filename") else args.pop("output_filename")

    tweet_id_list = read_input_csv(args.pop("filename"), url_col=args.pop("column"))

    output_lod = get_account_tweets(tweet_id_list)

    logging.info(f"Now writing to file: {output_filename}")
    write_output_csv(output_filename, output_lod)


#########################################################################################################
