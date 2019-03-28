#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import tweepy
import time
import re
import codecs


with open('authentication.json') as data_file:
    data = json.load(data_file)

auth = tweepy.OAuthHandler(consumer_key=data['nodos'][0]['consumer_key'],
                           consumer_secret=data['nodos'][0]['consumer_secret'])
auth.set_access_token(secret=data['nodos'][0]['access_token_secret'],key=data['nodos'][0]['access_token_key'])

api = tweepy.API(auth)

ids = []
for page in tweepy.Cursor(api.friends_ids, screen_name="asoc_celiaca_ar").pages():
    ids.extend(page)
    time.sleep(4)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

# USER properties
# {
#     'follow_request_sent': False,
#     'profile_use_background_image': True,
#     'id': 132728535,
#     '_api': < tweepy.api.api.object = ""at = ""xxxxxxx = "" >,
#     'verified': False,
#     'profile_sidebar_fill_color': 'C0DFEC',
#     'profile_text_color': '333333',
#     'followers_count': 80,
#     'protected': False,
#     'location': 'Seoul Korea',
#     'profile_background_color': '022330',
#     'id_str': '132728535',
#     'utc_offset': 32400,
#     'statuses_count': 742,
#     'description': "Cars, Musics, Games, Electronics, toys, food, etc... I'm just a typical boy!",
#     'friends_count': 133,
#     'profile_link_color': '0084B4',
#     'profile_image_url': 'http://a1.twimg.com/profile_images/1213351752/_2_2__normal.jpg',
#     'notifications': False,
#     'show_all_inline_media': False,
#     'geo_enabled': True,
#     'profile_background_image_url': 'http://a2.twimg.com/a/1294785484/images/themes/theme15/bg.png',
#     'screen_name': 'jaeeeee',
#     'lang': 'en',
#     'following': True,
#     'profile_background_tile': False,
#     'favourites_count': 2,
#     'name': 'Jae Jung Chung',
#     'url': 'http://www.carbonize.co.kr',
#     'created_at': datetime.datetime(2010, 4, 14, 1, 20, 45),
#     'contributors_enabled': False,
#     'time_zone': 'Seoul',
#     'profile_sidebar_border_color': 'a8c7f7',
#     'is_translator': False,
#     'listed_count': 2
# }



celiaq =  re.compile(u'celiaqu|tacc|gluten\s?free|gluten|#?sin\s?tacc|cel[ií]ac[oa]|libre\sde\sgluten', re.IGNORECASE)

users_data = {}
for user_chunk in chunks(ids,100):
    users = api.lookup_users(user_ids= [str(x) for x in user_chunk])

    # Por cada usuario
    for user in users:
        users_data[user.id] = {}
        users_data[user.id]['screen_name'] = user.screen_name
        users_data[user.id]['name'] = user.name
        users_data[user.id]['location'] = user.location
        users_data[user.id]['profile_description'] = user.description
        users_data[user.id]['tweet_count'] = user.statuses_count
        users_data[user.id]['followers_count'] = user.followers_count
        users_data[user.id]['friends_count'] = user.friends_count

        # Me fijo si la descripcion del usuario esta relacionada con el tema
        users_data[user.id]['profile_description_related'] = len(celiaq.findall(user.description)) > 0

        # Ahora me voy a fijar si en los ultimos 100 tweets hay algo relacionado con celiaquia
        users_data[user.id]['tweets'] = []
        users_data[user.id]['tweets_related'] = []
        try:
            twts = api.user_timeline(user_id=user.id, count=100, include_rts=True)
            for twt in twts:
                twt_data = {}
                twt_data['id'] = twt.id
                twt_data['text'] = twt.text
                twt_data['coordinates'] = twt.coordinates
                twt_data['lang'] = twt.lang
                twt_data['place'] = ''
                if twt.place != None:
                    twt_data['place'] = twt.place.country

                users_data[user.id]['tweets'].append(twt_data)
                if len(celiaq.findall(twt.text)) > 0:
                    users_data[user.id]['tweets_related'].append(twt.id)

            # Tiempo de espera por haber solicitado tweets de un usuario
            time.sleep(2)
        except Exception as e:
            print e.message
    # Tiempo de espera hasta buscar el proximo usuario
    time.sleep(2)

with codecs.open('users_saved.json', mode='w',encoding='utf-8') as us:
    json.dump(users_data,us)


