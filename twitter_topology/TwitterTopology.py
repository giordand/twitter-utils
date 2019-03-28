#!/usr/bin/env python
# -*- coding: utf-8 -*-

import networkx as nx
import json
import tweepy
import codecs
import os
from bson.json_util import dumps,STRICT_JSON_OPTIONS
from tweepy.error import TweepError

class ApiConnection(object):
    __twitter_api = None
    
    @staticmethod
    def getApiConnection(proxy=False, reset=False):
        if ApiConnection.__twitter_api == None or reset:
            if reset:
                print 'Reseteando la conexion'
            with codecs.open(os.path.dirname(__file__)+'/authentication.json', mode='r', encoding='utf-8') as data_file:
                auth_data = json.load(data_file)
    
            # Genero la conexion con tweepy
            auth = tweepy.OAuthHandler(consumer_key=auth_data['nodos'][0]['consumer_key'],
                                       consumer_secret=auth_data['nodos'][0]['consumer_secret'])
            auth.set_access_token(secret=auth_data['nodos'][0]['access_token_secret'],
                                  key=auth_data['nodos'][0]['access_token_key'])
    
            if proxy:
                api = tweepy.API(auth, proxy=auth_data['nodos'][0]['proxy'],wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
            else:
                api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
            ApiConnection.__twitter_api = api
    
        return ApiConnection.__twitter_api

def getFriends(user_id):
    api = ApiConnection.getApiConnection(proxy=True)
    fs = None

    while fs == None:
        try:
            fs = []
            for page in tweepy.Cursor(api.friends_ids, id=user_id).pages():
                fs.extend([str(x) for x in page])
                print len(fs)
        except TweepError as te:
            if te.api_code == 34:
                print 'El usuario ', user_id, 'no existe'
            elif te.response != None and te.response.status_code == 401:
                print 'No se puede acceder a los friends del usuario', user_id
            else:
                print te.message
                print te.reason
                fs = None
                api = ApiConnection.getApiConnection(proxy=True, reset=True)
    return fs

def getUsersFromTweets(tweets):
    '''

    :param tweets: lista de tweets a obtener sus usuarios
    :return: lista de usuarios con la estructura de Users de twitter --> https://dev.twitter.com/overview/api/users
    :rtype: list of dict
    '''
    usuarios = []
    usuarios_ids = []
    for t in tweets:
        usuario = t['user']
        if usuario['id'] not in usuarios_ids:
            usuarios_ids.append(usuario['id'])
            usuarios.append(usuario)

    return usuarios

def getUsersTopology(users,users_file=None,update_topology=False,user_dump_rate=1):
    '''
    :param users: lista de usuarios a obtener la topologia.
    :type users: list of dict

    :param users_file:  nombre del archivo donde ya hay datos de los usuarios cargados.
                        Por defecto None = para todos los usuarios solicita sus friends a la
                        API de twitter
    :type users_file: str

    :param update_topology: indica si se debe actualizar o no la lista de friends del usuario en el
                            archivo de usuarios indicado por el parametro users_file
    :type update_topology: bool

    :param user_dump_rate; cantidad de usuarios minimos antes de guardar los datos en el archivo de usuarios
    :type user_dump_rate: int

    :return: grafo donde la lista de usuarios (users) son los nodos y los links son representan la relacion (u1, 'follows',u2)
    :rtype: networkx.DiGraph
    '''

    users_data = {}
    if users_file != None and os.path.exists(os.path.dirname(__file__)+'/'+users_file):
        with codecs.open(os.path.dirname(__file__)+'/'+users_file, mode='r', encoding='utf-8') as data_file:
            users_data = json.load(data_file)


    # Genero la topologia
    # Agrego todos los usuarios al grafo
    G = nx.DiGraph()
    for u in users:
        if not G.has_node(u['id_str']):
            G.add_node(u['id_str'], attr_dict={'user': u['screen_name']})

    # Por cada usuario obtengo sus friends , primero busco en el archivo de usuarios sino voy a buscar
    # a la API de twitter
    ffetched_count = 0
    if users_file == None:
        users_file = 'tmp_users_dump.json'


    for u in users:
        friends_fetched = False
        if u['id_str'] in users_data:
            # Si el usuario esta en el archivo de usuarios
            u = users_data[u['id_str']]
            if update_topology:
                print 'Getting friends = ',u['screen_name']
                friends = getFriends(u['screen_name'])
                u['friends'] = friends
                users_data[u['id_str']] = u
                friends_fetched = True
        else:
            print 'Getting friends = ', u['screen_name']
            friends = getFriends(u['id'])
            u['friends'] = friends
            users_data[u['id_str']] = u
            friends_fetched = True



        if friends_fetched:
            ffetched_count += 1

        # Si la cantidad de usuarios para los que se solicito sus friends a la
        # API supera el maximo establecido por parametro, se guardan los usuarios
        if ffetched_count >= user_dump_rate:
            with codecs.open(os.path.dirname(__file__)+'/'+users_file, mode='w', encoding='utf-8') as data_file:
                #json.dump(data_file,users_data)
                data_file.write(dumps(users_data))
            ffetched_count = 0

        if 'friends' in u:
            for friend in u['friends']:
                if G.has_node(friend):
                    G.add_edge(u['id_str'], friend)
                    print 'Edge aded --> ', (u['id_str'], friend)


    return G

def getUsersFromFile(users_file):
    users_data = {}
    if users_file != None and os.path.exists(os.path.dirname(__file__)+'/'+users_file):
        with codecs.open(os.path.dirname(__file__)+'/'+users_file, mode='r', encoding='utf-8') as data_file:
            users_data = json.load(data_file)
    return users_data
