#!/usr/bin/env python
# _*_coding:utf-8_*_
import re,os
from nltk.tokenize import TweetTokenizer
import codecs

MAX_TERM_LEN = 30
def translate(to_translate):
    tabin = u'áéíóúÓò'
    tabout = u'aeiouoo'
    tabin = [ord(char) for char in tabin]
    translate_table = dict(zip(tabin, tabout))
    return to_translate.translate(translate_table)


emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>',  # HTML tags
    r'(?:@[\wñ_]+)',  # @-mentions
    r"(?:\#+[\wñ_]+[\wñ\'_\-]*[\wñ_]+)",  # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-zñ][a-zñ'\-_]+[a-zñ])",  # words with - and '
    r'(?:[\wñ_]+)',  # other words
    r'(?:\S)'  # anything else
]

emoticon_re = re.compile(r'^' + emoticons_str + '$', re.UNICODE)
url_re = re.compile(regex_str[4], re.UNICODE)
hashtags_re = re.compile(regex_str[3], re.UNICODE)
mentions_re = re.compile(regex_str[2], re.UNICODE)
tokens_re = re.compile(r'(' + '|'.join(regex_str[6:8]) + ')', re.UNICODE)


def stopwords():
    res = set()
    with codecs.open(os.path.dirname(__file__) + '/' +'stop.txt',mode='r',encoding='utf-8') as f:
        for l in f.readlines():
            l = re.sub('\n','',l)
            l = translate(l)
            l = l.lower()
            l = l.strip()
            res.add(l)
    return res

tknz = TweetTokenizer()
lista_stops = stopwords()
stops_re = re.compile(r'\b(' + '|'.join(lista_stops) + r')\b',re.UNICODE)

def updateDictionary(voc, tokens):
    for t in tokens:
        if t in voc:
            voc[t] += 1
        else:
            voc[t] = 1


def tweetsAsTokenizedDoc(tweets, avoidRT=False,avoidTW=False, removeStops = False):
    '''
    Devuelve una lista de terminos que represeta los tweets concatenados de los usuarios
    :param tweets: lista de tweets del usuario
    :param avoidRT: True para no tener en cuenta los retweets
    :param avoidTW: True para no tener en cuenta los tweets
    :param removeStops: True para quitar stopwords
    :return: lista de terminos que represeta los tweets concatenados de los usuarios
    :rtype: list of str
    '''
    doc = []
    for tweet in tweets:
        doc.extend(procTweets(tweet,avoidRT=avoidRT,avoidTW=avoidTW,removeStops=removeStops))
    return doc


def preprocTweet(tw):

    tweet = tw.lower()
    return translate(tweet)


def procTweets(tweet,avoidRT=False,avoidTW=False,removeStops=False):

    tweet = preprocTweet(tweet['text'])

    procTW = False
    if tweet[0:2] != 'rt' and not avoidTW:
        procTW = True
    elif tweet[0:2] == 'rt' and not avoidRT:
        procTW = True

    if procTW:
        if removeStops:
            tweet = stops_re.sub('', tweet)

        tokens = termsExtract(tweet,includeUrl=False)
        return tokens
    return []


def termsExtract(tweet,includeUrl=True,includeMentions=True,includeHashtags=True):

    tweet = tweet.lower()
    tweet = translate(tweet)
    # Hashtags
    hs = hashtags_re.findall(tweet)
    tweet = hashtags_re.sub(' ', tweet)

    # Menciones
    ms = mentions_re.findall(tweet)
    tweet = mentions_re.sub(' ', tweet)

    # urls
    us = url_re.findall(tweet)
    tweet = url_re.sub(' ', tweet)

    # Otros
    tokens = tokens_re.findall(tweet)
    res = includeHashtags*hs + includeMentions*ms + includeUrl*us + tokens
    for t in res:
        if len(t) > MAX_TERM_LEN:
            res.remove(t)
    return res