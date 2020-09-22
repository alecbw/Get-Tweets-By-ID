from utility.util import split_list_to_fixed_length_lol


from time import sleep
from datetime import datetime, timedelta
import csv
import os
import sys
import argparse
import logging

logging.getLogger().setLevel(logging.INFO)

try:
    import twint
    import gspread
    from google.oauth2.service_account import Credentials
    from google.oauth2 import service_account
except ImportError:
    sys.exit("~ Make sure you install twint. Run `pip install twint google-auth gspread` and try this again ~")


argparser = argparse.ArgumentParser()
argparser.add_argument('-k', '--keywords', nargs='+', help='<Required> A list of keywords (separated by spaces) that you want to search for', required=True)
argparser.add_argument('-o', '--output_filename', nargs='?', default="! Resulting Tweets.csv", help="If you want an output filename other than the default")
argparser.add_argument('-g', '--output_gsheet', nargs='?', help="Write to Google Sheets with the spreadsheet name you specify")
argparser.add_argument('-d', '--deduplicate', nargs='?', default="! Resulting Tweets.csv", help="If you want an output filename other than the default")
argparser.add_argument('-s', '--since', nargs='?', default=None, help="If you want to filter by posted date since a given date. Format is 2019-12-20 20:30:15")
argparser.add_argument('-u', '--until', nargs='?', default=None, help="If you want to filter by posted date until a given date. Format is 2019-12-20 20:30:15")
argparser.add_argument('-l', '--limit', nargs='?', default=None, help="If you want to limit the results per keyword provided")
argparser.add_argument('-m', '--min_likes', nargs='?', default=None, help="If you want to limit the results to only tweets with a given number of likes")
argparser.add_argument('-n', '--near', nargs='?', default=None, help="If you want to limit the results to tweets geolocated near a given city")
argparser.add_argument('-v', '--verified', nargs='?', default=None, help="If you want to limit the results to tweets made by accounts that are verified")
argparser.add_argument('-q', '--hide_output', nargs='?', default=True, help="If you want to disable routing results logging")
argparser.add_argument('-r', '--resume', nargs='?', default=None, help="Have the search resume at a specific Tweet ID")

args = argparser.parse_args()
args = vars(args) # convert to dict


######################## GSheets Helpers #########################################


# def auth_gspread():
#     if not os.getenv("GSHEETS_PRIVATE_KEY"):
#         sys.exit("!!! Make sure you have set up Google Sheets auth and have set the GSHEETS_PRIVATE_KEY and GSHEETS_CLIENT_EMAIL as environment variables")
    
#     auth = {
#         "private_key": os.environ["GSHEETS_PRIVATE_KEY"].replace("\\n", "\n").replace('"', ''),
#         "client_email": os.environ["GSHEETS_CLIENT_EMAIL"],
#         "token_uri": "https://oauth2.googleapis.com/token",
#     }
#     scopes = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]
#     credentials = Credentials.from_service_account_info(auth, scopes=scopes)
#     gc = gspread.authorize(credentials)
#     return gc


# def write_new_google_sheet(result_lod, output_filename):
#     sh = gc.open(output_filename)
#     tab = sh.get_worksheet(0)  # get the first tab

#     tab.update([list(result_lod[0].keys())] + [list(x.values()) for x in result_lod])

#     tab.resize(
#         rows=len(result_lod),
#         cols=len(result_lod[0])
#     )
#     logging.info(f"Successful write to Google Sheet {output_filename}")
    

###############################################################################


def deduplicate_lod(input_lod, primary_key):
    if not primary_key:
        output_lod = {json.dumps(d, sort_keys=True) for d in input_lod}  # convert to JSON to make dicts hashable
        return [json.loads(x) for x in output_lod]                 # unpack the JSON

    output_dict = {}
    for d in input_lod:
        if d.get(primary_key) not in output_dict.keys():
            output_dict[d[primary_key]] = d

    return list(output_dict.values())



def get_account_tweets(tweet_id_list):
    auth_dict = json.loads(os.environ["TW_KEYS"])
    auth = tweepy.OAuthHandler(auth_dict["TW_CONSUMER_KEY"], auth_dict["TW_CONSUMER_SECRET"])
    auth.set_access_token(auth_dict["TW_ACCESS_KEY"], auth_dict["TW_ACCESS_SECRET"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    tweet_ids_lol = split_list_to_fixed_length_lol(tweet_id_list, 100)
    for tweet_id_slice in tweet_ids_lol:
      alltweets = [] # Initialize a list to hold all the tweepy Tweets
      # new_tweets = api.user_timeline(screen_name=params["Account"], count=200, tweet_mode="extended")  # Make initial request for most recent tweets (200 is the maximum allowed count)
      tweets = API.statuses_lookup(tweet_id_list)
      alltweets.extend(tweets)  # Save most recent tweets

        # new_tweets = api.user_timeline(screen_name=params["Account"], count=20, tweet_mode="extended", max_id=oldest) # All subsequent requests use the max_id param to prevent duplicates
        # alltweets.extend(new_tweets) # Save most recent tweets TODO count 200 here?
        # oldest = alltweets[-1].id - 1 # Update the id of the oldest tweet less one

        # logging.info(f"{len(alltweets)} tweets downloaded so far")

        # if len(alltweets) > int(params.get("Limit", 3240)):
        #     logging.info(f"Hit provided limit {params['Limit']}")
        #     break

    # Then unpack the object into a nice, inoffensive list of dictionaries
    output_lod = [{
        'author_account': tweet.author.screen_name,
        'author_name': tweet.author.name,
        'author_description': tweet.author.description,
        'author_location': tweet.author.location,
        'author_url': tweet.author.url,
        'created_at': tweet.created_at.strftime("%m/%d/%Y"),
        'favorite_count': tweet.favorite_count,
        'geo': tweet.geo,
        'id': tweet.id,
        'in_reply_to_screen_name': tweet.in_reply_to_screen_name,
        'in_reply_to_status_id': tweet.in_reply_to_status_id,
        'in_reply_to_user_id': tweet.in_reply_to_user_id,
        'is_quote_status': tweet.is_quote_status,
        'lang': tweet.lang,
        'retweet_count': tweet.retweet_count,
        'source': tweet.source,
        'text': tweet.full_text
    }
     for tweet in alltweets]

    return output_lod

# def unpack_twint_tweet(keyword, tweet_list):

#     output_lod = []

#     for n, tweet in enumerate(tweet_list):
#         output_dict = {
#             'tweet_id': tweet.id,
#             'keyword_searched': keyword,
#             'author_account': tweet.username,
#             'author_name': tweet.name,
#             'author_id': tweet.user_id,
#             'created_at': datetime.strftime(datetime.fromtimestamp(tweet.datetime/1000), '%Y-%m-%d %H:%M:%S'),
#             'timezone': tweet.timezone,
#             'geo': tweet.place or tweet.near, # geo may not be working TODO
#             'text': tweet.tweet,
#             'link': tweet.link,
#             'urls': ", ".join(tweet.urls),
#             'mentions': ", ".join(tweet.mentions),
#             'likes': tweet.likes_count,
#             'retweets': tweet.retweets_count,
#             'replies': tweet.replies_count,
#             'in_reply_to_screen_name': ", ".join([x.get('username') for x in tweet.reply_to]) if tweet.reply_to else None,
#             'QT_url': tweet.quote_url,
#             # 'RT': tweet.retweet,f
#             # 'user_rt': tweet.user_rt,
#             # 'user_rt_id': tweet.user_rt_id,
#             # 'rt_id': tweet.retweet_id,
#             # 'user_rt_date': tweet.retweet_date,
#         }
#         output_lod.append(output_dict)

#     return output_lod


# def twint_scrape(keyword, args):
#     c = twint.Config()
#     c.Search = keyword

#     for k,v in args.items():
#         if isinstance(v, str) and v.title() == "False":  # argparse converts to str
#             setattr(c, k.capitalize(), False)
#         elif v:
#             setattr(c, k.capitalize(), v)

#     tweets = []
#     c.Store_object = True  # preferable to using twint.output.tweets_list
#     c.Store_object_tweets_list = tweets

#     # c.Resume = "scrape_interrupted_last_id.csv" # TODO implement save-last-scroll-id
#     twint.run.Search(c)

#     output_lod = unpack_twint_tweet(keyword, tweets) 

#     logging.info(f"The keyword {keyword} has produced {len(output_lod)} tweets")
#     return output_lod


if __name__ == "__main__":
    result_lod = []
    deduplicate_option = args.pop("deduplicate")

    if args.get("output_gsheet", False):
        output_filename = args.pop("output_gsheet")
        gc = auth_gspread()
    else:
        output_filename = args.pop("output_filename") + ".csv" if ".csv" not in args.get("output_filename") else args.pop("output_filename")

    for keyword in args.pop("keywords"):
        logging.info(f"Now processing keyword {keyword}")
        result_lod += twint_scrape(keyword, args)

    if deduplicate_option:
        logging.info("Deduplicating the results")
        result_lod = deduplicate_lod(result_lod, 'tweet_id')

    if ".csv" in output_filename:
        logging.info(f"Now writing to file: {output_filename}")
        with open(output_filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, result_lod[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(result_lod)
    else:
        write_new_google_sheet(result_lod, output_filename)


#########################################################################################################
