#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from ..configurations import getLog, LOGS_PATH, CELIAQUIA_URL, CELIAQUIA_COLLECTION, CELIAQUIA_DATABASE
import logging

bkp_json_file = 'tweetsBackup.json'

logger = getLog(__name__, LOGS_PATH + __name__ + '.log')


def getMongoClient():
    return MongoClient(CELIAQUIA_URL)


def getTweets(source='db'):
    logger.log(logging.INFO,'Obteniendo tweets. Source = '+source)
    try:
        if source == 'db':
            client = MongoClient(CELIAQUIA_URL)
            tweets = client.get_database(CELIAQUIA_DATABASE).get_collection(CELIAQUIA_COLLECTION).find()
            logger.log(logging.INFO, 'Obtenidos los tweets de la base de datos')
            return tweets
        elif source == 'bk':
            import json
            import codecs
            with codecs.open(bkp_json_file, mode='r', encoding='utf-8') as us:
                logger.log(logging.INFO, 'Leyendo backup de tweets')
                return json.load(us)
    except Exception as e:
        logger.log(logging.ERROR, e.message)


def backupDB():
    try:
        from bson.json_util import dumps
        import codecs
        logger.log(logging.INFO,'Iniciando backup de base de datos')
        tweets = getTweets()
        with codecs.open('tweets_bkp.json', mode='w', encoding='utf-8') as fb:
            fb.write(dumps(tweets))
        print 'Backup finalizado'
        logger.log(logging.INFO, 'Backup finalizado')
    except Exception as e:
        logger.log(logging.ERROR, e.message)
        print e
