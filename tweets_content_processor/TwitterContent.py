#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twitter_utils.twitter_topology.TwitterTopology import ApiConnection
from TwitterText import tweetsAsTokenizedDoc,updateDictionary
from MatchingModels import cosineSimilarity
import os
import codecs
import json
from bson.json_util import dumps
from tweepy.error import TweepError
from ..configurations import USERS_TIMELINE_PATH,LOGS_PATH,USERS_TEXTSIM_FILE,getLog,USERS_DOCS_FILE,CONCAT_USERS_DOCS_FILE
import logging

logger = getLog(__name__,LOGS_PATH + __name__ + '.log')

TWEETS_FILE_PREFIX = 'tweets'
USERS_DUMP_PREFIX = 'users_dumped'


def get_all_tweets(user_screen_name):
    # Twitter only allows access to a users most recent 3240 tweets with this method
    logger.log(logging.INFO, "Procesando -->"+str(user_screen_name))
    api = ApiConnection.getApiConnection(proxy=True)
    alltweets = None
    while alltweets == None:
        try:
            # initialize a list to hold all the tweepy Tweets
            alltweets = []

            # make initial request for most recent tweets (200 is the maximum allowed count)
            new_tweets = api.user_timeline(screen_name=user_screen_name, count=200)

            # save most recent tweets
            for tw in new_tweets:
                alltweets.append(tweetToDict(tw))


            #oldest = alltweets[-1]['id']- 1

            # keep grabbing tweets until there are no tweets left to grab
            while len(new_tweets) > 0:

                # save the id of the oldest tweet less one
                oldest = alltweets[-1]['id'] - 1

                logger.log(logging.INFO, "getting tweets before %s" % (oldest))

                # all subsiquent requests use the max_id param to prevent duplicates
                new_tweets = api.user_timeline(screen_name=user_screen_name, count=200, max_id=oldest)

                # save most recent tweets
                for tw in new_tweets:
                    alltweets.append(tweetToDict(tw))

                # update the id of the oldest tweet less one

                logger.log(logging.INFO, "...%s tweets downloaded so far" % (len(alltweets)))

            return alltweets

        except TweepError as te:
            if te.api_code == 34:
                logger.log(logging.ERROR, 'El usuario '+str(user_screen_name)+'no existe')
            elif te.response != None and te.response.status_code == 401:
                logger.log(logging.ERROR, 'No se puede acceder a los friends del usuario '+ user_screen_name)
            else:
                logger.log(logging.ERROR, str(te.message) )
                logger.log(logging.ERROR, te.reason)
                alltweets = None
                logger.log(logging.WARNING, 'Reseteando la conexion')
                api = ApiConnection.getApiConnection(proxy=True,reset=True)

def tweetToDict(tw):
    status = {'created_at': '',
              'Id': '',
              "id_str": '',
              "text": '',
              # (There are many more fields in the Tweet Model - check out the above Twitter document.

              # The User Model follows the model described in https://dev.twitter.com/overview/api/users
              "user": {"id": '',
                       "id_str": '',
                       "contributors_enabled": '',
                       "name": '',
                       "screen_name": '',
                       # (Again, there are many more fields in the model, we won't cover them all.)
                       },
              }

    # Here we retrive the actual Tweet values from the tweet object.
    # Note that Twitter recommends that applications should be able to handle any
    # missing fields in the Tweet - so we use the getattr method to detect any non-existing
    # attributes and replace them with a default value ('Does not Exist')

    attributes = ['created_at', 'id', 'id_str', 'text']

    for attribute in attributes:
        status[attribute] = getattr(tw, attribute, 'Does Not Exist')

    original_tweet = getattr(tw, 'retweeted_status', None)
    if original_tweet != None:
        status['retweeted_status'] = tweetToDict(original_tweet)
    user_attributes = ['id', 'id_str', 'contributors_enabled', 'name', 'screen_name']

    for user_attribute in user_attributes:
        status['user'][user_attribute] = getattr(tw.user, user_attribute, 'Does Not Exist')

    status['entities'] = getattr(tw, 'entities', 'Does Not Exist')

    # Go over the keys, print the value.
    # if the value is a dict - recuse with it's keys.

    return status

def dumpUsersTimeline(users_screen_names, read_users_dumped=True,user_dump_rate=50):
    '''

    :param users_screen_names: lista de los ids de los usuarios a obtener sus tweets
    :type users_screen_names: list of str
    :param tweets_file: archivo JSON donde seran almacenados los tweets
    :type tweets_file: str
    :param user_dump_rate: numero que indica cada cuantos usuarios se actualiza el archivo de tweets
    '''

    users_dumped = list()
    dump_number = 1
    if read_users_dumped:
        while os.path.isfile(USERS_TIMELINE_PATH + USERS_DUMP_PREFIX +str(dump_number)):
            with codecs.open(USERS_TIMELINE_PATH + USERS_DUMP_PREFIX +str(dump_number), mode='r', encoding='utf-8') as data_file:
                users = data_file.readline()
                users_dumped.extend(users.split(';'))
            dump_number+=1

    tweets_data = {}
    tweets_fetched  = 0
    new_users = list()
    for u_id in users_screen_names:
        if u_id not in users_dumped:
            tweets = get_all_tweets(u_id)
            tweets_data[u_id] = tweets
            new_users.append(u_id)

            tweets_fetched += 1
            if tweets_fetched >= user_dump_rate:
                logger.log(logging.INFO,'Dumping nro '+str(dump_number))
                saveUsersTimeline(TWEETS_FILE_PREFIX + str(dump_number), tweets_data, USERS_DUMP_PREFIX + str(dump_number), new_users)
                dump_number += 1
                new_users = list()
                tweets_fetched = 0
                tweets_data = {}

    # Guardo los usuarios restantes (los que no entraron en el ultimo bloque de 50)
    if tweets_fetched>0:
        logger.log(logging.INFO, 'Dumping nro ' + str(dump_number))
        saveUsersTimeline(TWEETS_FILE_PREFIX + str(dump_number), tweets_data, USERS_DUMP_PREFIX + str(dump_number),
                          new_users)

def readUsersTimeline(dump_number):
    '''

    :param tweets_file:
    :return:
    :rtype: dict
    '''
    data = None
    if os.path.isfile(USERS_TIMELINE_PATH + TWEETS_FILE_PREFIX + str(dump_number)):
        with codecs.open(USERS_TIMELINE_PATH + TWEETS_FILE_PREFIX + str(dump_number),
                             mode='r', encoding='utf-8') as data_file:
            data = json.load(data_file)
        data_file.close()
    return data

def getDumpsNumber():
    dump_number = 1
    while os.path.isfile(USERS_TIMELINE_PATH + TWEETS_FILE_PREFIX + str(dump_number)):
        dump_number += 1
    return  dump_number - 1

def computeUsersSimilarity(readUserDocs=True):
    logger.log(logging.INFO, 'computeUsersSimilarity()')
    import itertools

    try:
        #Obtengo la linea de tiempo de todos los usuarios

        if not readUserDocs:
            users_doc = generateUsersDocsFile()
        else:
            users_doc = getUsersDocsFile()


        # Calculo todas las combinaciones posibles entre los usuarios, esto representa todas las
        # aristas posibles entre los nodos de la red.

        logger.log(logging.INFO, 'Cantidad de usuarios ='+str(len(users_doc.keys())))
        users = users_doc.keys()
        pairs = [x for x in itertools.combinations(users, 2)]
        logger.log(logging.INFO, 'Cantidad combinaciones ='+str(len(pairs)))

        with codecs.open(USERS_TEXTSIM_FILE, mode='w', encoding='utf-8') as data_file:
            count = 0
            # Por cada arista posible (par de usuarios) calculo su similitud.
            for u1,u2 in pairs:
                # Si se logro generar el documento para ambos usuarios se podrá calcular su similitud
                if u1 in users_doc and u2 in users_doc:
                    # Calculo la similitud por coseno

                    dict_u1 = users_doc[u1]
                    dict_u2 = users_doc[u2]

                    similitud = cosineSimilarity(dict_u1.copy(),dict_u2.copy(),u1,u2)

                    data_file.write(u1 + ',' + u2 + ',' + str(repr(similitud)) + '\n')

                if count % 500 == 0 and count != 0:
                    logger.log(logging.INFO, str(count)+' pares procesados, '+str((count/float(len(pairs)))*100)+'% completado...')
                count += 1

        logger.log(logging.INFO, 'Scores por usuarios guardados en ' + USERS_TEXTSIM_FILE)

    except Exception as e:
        import sys,traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.log(logging.ERROR, 'Exception:\n'+str(e))
        logger.log(logging.ERROR, exc_traceback)
        logger.log(logging.ERROR, exc_value.args[0])

def user_similarity_worker(pairs,users_doc,proc_id):
    #logger = getLog(__name__+'worker_'+str(proc_id), LOGS_PATH + __name__ + '_user_similarity_worker_'+str(proc_id)+'.log')
    #print 'Ejecutando proc_id',proc_id
    count = 0
    users_scores = []
    for u1, u2 in pairs:
        # Si se logro generar el documento para ambos usuarios se podrá calcular su similitud
        if u1 in users_doc and u2 in users_doc:
            dict_u1 = users_doc[u1]
            dict_u2 = users_doc[u2]

            similitud = cosineSimilarity(dict_u1.copy(), dict_u2.copy(), u1, u2)
            users_scores.append((u1, u2, similitud))

        if count % 5000 == 0 and count != 0:
            print 'Proceso '+str(proc_id)+' = '+str(count) + ' pares procesados, ' + str((count / float(len(pairs))) * 100) + '% completado...'
        count += 1

    return users_scores

def computeUsersSimilarityParallel(readUserDocs=True):
    logger.log(logging.INFO, 'computeUsersSimilarityParallel()')
    import itertools
    from twitter_utils.Utils import chunks
    import multiprocessing as mp

    # Diccionario que almacena similitudes por aristas. Estructura = {(usuario1,usuario2):similitud}
    users_scores = []
    try:
        # Obtengo la linea de tiempo de todos los usuarios

        if not readUserDocs:
            users_doc = generateUsersDocsFile()
        else:
            users_doc = getUsersDocsFile()

        # Calculo todas las combinaciones posibles entre los usuarios, esto representa todas las
        # aristas posibles entre los nodos de la red.

        logger.log(logging.INFO, 'Cantidad de usuarios ='+str(len(users_doc.keys())))
        users = users_doc.keys()
        pairs = [x for x in itertools.combinations(users, 2)]
        logger.log(logging.INFO, 'Cantidad combinaciones ='+str(len(pairs)))


        # Por cada arista posible (par de usuarios) calculo su similitud.
        proc_num = 20

        pairs_chunks = chunks(pairs,len(pairs)/proc_num)
        pool = mp.Pool(processes=proc_num+1)
        proc_id = 1
        results = []
        for pairs_c in pairs_chunks:
            print 'Proc id = '+str(proc_id)
            results.append(pool.apply_async(user_similarity_worker, args=(pairs_c,users_doc,proc_id)))
            proc_id += 1
        pool.close()
        pool.join()

        # Junto los resultados de los procesos
        for r in results:
            users_scores_proc = r.get()
            users_scores.extend(users_scores_proc)

        logger.log(logging.INFO, 'Cantidad similitudes calculadas =' + str(len(users_scores)))

        with codecs.open(USERS_TEXTSIM_FILE, mode='w', encoding='utf-8') as data_file:
            import csv
            logger.log(logging.INFO, 'Guardando scores')
            writer = csv.writer(data_file, delimiter=',', lineterminator='\n')
            writer.writerows(users_scores)
            logger.log(logging.INFO, 'Scores por usuarios guardados en ' + USERS_TEXTSIM_FILE)

    except Exception as e:
        import sys,traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print e
        print traceback.format_exception(exc_type, exc_value,
                                          exc_traceback)
        logger.log(logging.ERROR, 'Exception:\n'+str(e))
        logger.log(logging.ERROR, exc_traceback)
        logger.log(logging.ERROR, exc_value.args[0])

def generateUsersDocsFile():
    logger.log(logging.INFO, 'generateUsersDocsFile()')
    users_doc = {}
    for i in xrange(1, getDumpsNumber() + 1):
        logger.log(logging.INFO, 'Leyendo timeline ' + str(i))
        timeline = readUsersTimeline(i)
        for u, tws in timeline.iteritems():
            if tws != None and len(tws) > 0:
                # Genero un documento con todos los tweets de cada usuario
                logger.log(logging.INFO, 'Generando documento para usuario ' + u)
                #users_doc[u] = tweetsAsTokenizedDoc(tws, avoidRT=True, removeStops=True)
                tokenized_doc = tweetsAsTokenizedDoc(tws, avoidRT=True, removeStops=True)

                if len(tokenized_doc) > 0:
                    # Genero el diccionario {'termino':frecuencia} para el usuario
                    logger.log(logging.INFO, 'Generando diccionario {\'termino\':frecuencia} para usuario ' + u)
                    dict_u = {}
                    updateDictionary(dict_u, tokenized_doc)
                    users_doc[u] = dict_u

    with codecs.open(USERS_DOCS_FILE, mode='w', encoding='utf-8') as data_file:
        data_file.write(dumps(users_doc))
    return users_doc

def generateConcatenatedUserDocFile():
    logger.log(logging.INFO, 'generateConcatenatedUsersDocsFile()')
    users_doc = {}
    for i in xrange(1, getDumpsNumber() + 1):
        logger.log(logging.INFO, 'Leyendo timeline ' + str(i))
        timeline = readUsersTimeline(i)
        for u, tws in timeline.iteritems():
            if tws != None and len(tws) > 0:
                # Genero un documento con todos los tweets de cada usuario
                logger.log(logging.INFO, 'Generando documento concatenado para usuario ' + u)
                tokenized_doc = tweetsAsTokenizedDoc(tws, avoidRT=True, removeStops=True)

                if len(tokenized_doc) > 0:
                    users_doc[u] = ' '.join(tokenized_doc)

    with codecs.open(CONCAT_USERS_DOCS_FILE, mode='w', encoding='utf-8') as data_file:
        data_file.write(dumps(users_doc))
    return users_doc

def getConcatenatedUserDocFile():
    logger.log(logging.INFO, 'getConcatenatedUserDocFile()')
    with codecs.open(CONCAT_USERS_DOCS_FILE, mode='r', encoding='utf-8') as data_file:
        return json.load(data_file)

def getUsersDocsFile():
    logger.log(logging.INFO, 'getUsersDocsFile()')
    with codecs.open(USERS_DOCS_FILE, mode='r', encoding='utf-8') as data_file:
        return json.load(data_file)

def saveUsersTimeline(tweets_file,tweets_data,users_dump_file,users_dumped):
    with codecs.open(USERS_TIMELINE_PATH + tweets_file, mode='w', encoding='utf-8') as data_file:
        data_file.write(dumps(tweets_data))
    with codecs.open(USERS_TIMELINE_PATH + users_dump_file, mode='w', encoding='utf-8') as data_file:
        data_file.write(';'.join(users_dumped))

def getUsersSimilarity():
    import csv
    '''

    :return:
    :rtype: dict
    '''
    with codecs.open(USERS_TEXTSIM_FILE, mode='r', encoding='utf-8') as data_file:
        reader = csv.reader(data_file, delimiter=',', lineterminator='\n')
        users_sim = []
        for row in reader:
            users_sim.append([row[0],row[1],float(row[2])])
        return users_sim

def getUsersTimeline(users):
    logger.log(logging.INFO, 'getUsersTimeline()')
    res = {}
    for i in xrange(1, getDumpsNumber() + 1):
        logger.log(logging.INFO, 'Leyendo timeline ' + str(i))
        timeline = readUsersTimeline(i)
        for u, tws in timeline.iteritems():
            if u in users:
                res[u] = tws
    return res