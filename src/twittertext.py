# twittertext.py
#   An attempt to translate an open-source Java library provided by Twitter
#   for the purpose of extracting information from tweet text.
#   
#   Original library: https://github.com/twitter/twitter-text/tree/master/java/src/main/java/com/twitter/twittertext
#
#   Also includes: 
#    - a function from external package `tweet_counter` to count the characters of a tweet
#    - a function from external package `tldextract` to extract top-level domain from a URL


import re
import pandas as pd
import tldextract       # Source: https://github.com/john-kurkowski/tldextract

from tweet_counter import count_tweet   
    # Source: https://github.com/nottrobin/tweet-counter
    #   Version: 0.1.0
    #   Note: on Windows, above package requires manual installation (`pip install` generates error)


__all__ = ['char_count', 'retweet_handle', 'reply_handle', 'explode_url']


# constants
#   based on Regex.UNICODE_SPACES
UNICODE_SPACES: str = "".join([
    "[",
    "\u0009-\u000d",    #  # White_Space # Cc   [5] <control-0009>..<control-000D>
    "\u0020",           # White_Space # Zs       SPACE
    "\u0085",           # White_Space # Cc       <control-0085>
    "\u00a0",           # White_Space # Zs       NO-BREAK SPACE
    "\u1680",           # White_Space # Zs       OGHAM SPACE MARK
    "\u180E",           # White_Space # Zs       MONGOLIAN VOWEL SEPARATOR
    "\u2000-\u200a",    # # White_Space # Zs  [11] EN QUAD..HAIR SPACE
    "\u2028",           # White_Space # Zl       LINE SEPARATOR
    "\u2029",           # White_Space # Zp       PARAGRAPH SEPARATOR
    "\u202F",           # White_Space # Zs       NARROW NO-BREAK SPACE
    "\u205F",           # White_Space # Zs       MEDIUM MATHEMATICAL SPACE
    "\u3000",           # White_Space # Zs       IDEOGRAPHIC SPACE
    "]"
  ])

#   based on Regex.DIRECTIONAL_CHARACTERS
DIRECTIONAL_CHARACTERS: str = "".join([
    "\u061C",   # ARABIC LETTER MARK (ALM)
    "\u200E",   # LEFT-TO-RIGHT MARK (LRM)
    "\u200F",   # RIGHT-TO-LEFT MARK (RLM)
    "\u202A",   # LEFT-TO-RIGHT EMBEDDING (LRE)
    "\u202B",   # RIGHT-TO-LEFT EMBEDDING (RLE)
    "\u202C",   # POP DIRECTIONAL FORMATTING (PDF)
    "\u202D",   # LEFT-TO-RIGHT OVERRIDE (LRO)
    "\u202E",   # RIGHT-TO-LEFT OVERRIDE (RLO)
    "\u2066",   # LEFT-TO-RIGHT ISOLATE (LRI)
    "\u2067",   # RIGHT-TO-LEFT ISOLATE (RLI)
    "\u2068",   # FIRST STRONG ISOLATE (FSI)
    "\u2069",   # POP DIRECTIONAL ISOLATE (PDI)
    ])

#   based on Regex.AT_SIGNS_CHAR and Regex.AT_SIGNS
AT_SIGNS: str = "[@\uFF20]"


#   VALID_REPLY regex pattern
#   Java: `VALID_REPLY = Pattern.compile("^(?:" + UNICODE_SPACES + "|" + DIRECTIONAL_CHARACTERS + ")*" + AT_SIGNS + "([a-z0-9_]{1,20})", Pattern.CASE_INSENSITIVE);`
VALID_REPLY_PATTERN_STR = "^(?:" + UNICODE_SPACES + "|" + DIRECTIONAL_CHARACTERS + ")*" + AT_SIGNS + "([a-z0-9_]{1,20})"
VALID_REPLY_PATTERN = re.compile(VALID_REPLY_PATTERN_STR, flags=re.IGNORECASE)


# functions
##########################
#### PUBLIC INTERFACE ####
##########################

def char_count(tweet_series: pd.Series) -> int:
    """Counts the number of characters in a tweet using an approximation of Twitter's specific counting method."""
    return _get_tweet_char_count(tweet_series['content'])


def retweet_handle(tweet_series: pd.Series) -> str:
    """Returns the Twitter handle of the parent author of a retweeted tweet.
        E.g. if @foo is retweeting a tweet by @bar with the text `RT @bar What is your name?`, returns `bar`."""
    handle: str = _extract_first_handle_after_RT(tweet_series['content'])
    if (handle is None):
        return None
    else:
        return handle.lstrip('@')


def reply_handle(tweet_series: pd.Series) -> str:
    """Returns the Twitter handle of the parent author of a reply tweet.
        E.g. if @foo is replying to a tweet by @bar with the text `@bar What is your quest?`, returns `bar`."""
    handle: str = _extract_reply_screenname(tweet_series['content'])
    if (handle is None):
        return None
    else:
        return handle.lstrip('@')


def explode_url(url_text: str) -> dict:
    """Breaks apart a string containing only a fully-qualified URL.
        Returns the pieces of the URL as a dict. Refer to tldextract docs
        for more information."""
    url_named_tuple: tldextract.ExtractResult = tldextract.extract(url_text)

    return {
            'subdomain': url_named_tuple.subdomain,
            'domain': url_named_tuple.domain,
            'tld': url_named_tuple.suffix,
            'registered_domain': url_named_tuple.registered_domain
        }


###########################
#### PRIVATE FUNCTIONS ####
###########################

def _extract_reply_screenname(tweet_text: str):
    """Returns the handle, including `@`, occuring at start of a tweet (i.e. when a tweet is replying to another).
        Returns None if anything but a handle is at the very start of a tweet.
        Regex based on Twitter's `twittertext` code."""
    return VALID_REPLY_PATTERN.match(tweet_text)    # seems to work, may consider "search" but "match" looks only at start of string


def _extract_first_handle_after_RT(tweet_text: str):
    """Returns the handle, including `@`, occuring at start of a tweet but after chars 'RT ' (note the trailing space).
        Returns None if anything but 'RT ' followed by a handle is at the very start of a tweet.
        Note that check for 'RT ' is expected to be performed prior to calling this function.
        Regex based on Twitter's `twittertext` code."""
    return VALID_REPLY_PATTERN.search(tweet_text[3:])   # seems to work


def _get_tweet_char_count(tweet_text: str) -> int:
    """Counts the number of characters in a Tweet using an approximation of the method used by Twitter.
        Twitter determines character count using a very specific approach: https://developer.twitter.com/en/docs/counting-characters"""
    return count_tweet(tweet_text)


if __name__ == '__main__':
    pass
