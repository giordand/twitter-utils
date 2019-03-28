#!/usr/bin/env python
# -*- coding: utf-8 -*-


import googlemaps.geolocation
import json
import codecs
import re
import time
import os


def translate(to_translate):
    tabin = u'áéíóú'
    tabout = u'aeiou'
    tabin = [ord(char) for char in tabin]
    translate_table = dict(zip(tabin, tabout))
    return to_translate.translate(translate_table)


provincias = codecs.open(os.path.dirname(__file__) + '/provincias.txt', mode='r', encoding='utf-8').readlines()
localidades = codecs.open(os.path.dirname(__file__) + '/localidades.txt', mode='r', encoding='utf-8').readlines()

abreviaturas = [u' caba ', u' arg ', u' bs. as. ', u' bsas ', u' bs.as. ', u' gba ', u' ar ']

completos = [u'argentina', u'ciudad autonoma de buenos aire', u'ciudad autonoma de buenos aires']

provs_data = {}
locs_data = {}

for x in provincias:
    x = translate(re.sub('\n', '', x).lower())
    x = x.split(';')

    provs_data[x[0]] = x[1]

for x in localidades:
    x = translate(re.sub('\n', '', x).lower())
    x = x.split(';')

    if x[0] in locs_data:
        locs_data[x[0]].append(x[1])
    else:
        locs_data[x[0]] = [x[1]]

provs = list(provs_data.itervalues())
locs = list(locs_data.iterkeys())


class TweetLocations(object):

    def __init__(self, googleClientKeyApi, https_proxy=None, http_proxy=None):
        locations_file = codecs.open(os.path.dirname(__file__) + '/locations.json', mode='r', encoding='utf-8')

        self.__locations = json.load(locations_file)

        self.__geocodesCount = 0

        self.__tiposNoPermitidos = ['extraDefined', 'encAbreviaturas']

        proxies = None
        if https_proxy is not None or http_proxy is not None:
            proxies = {'proxies': {}
                       }
            if https_proxy is not None:
                proxies['proxies']['https'] = https_proxy
            if http_proxy is not None:
                proxies['proxies']['http'] = http_proxy

        if proxies is not None:
            self.__gmaps = googlemaps.Client(key=googleClientKeyApi, requests_kwargs=proxies)
        else:
            self.__gmaps = googlemaps.Client(key=googleClientKeyApi)

    def getGmapClient(self):
        return self.__gmaps

    @staticmethod
    def locationMerge(splitted):
        merge = ''
        for ele in splitted:
            if ele != None and ele != '':
                merge += ele + ','
        return merge[:-1]

    @staticmethod
    def completarProvincia(splitted):
        localidad = splitted[0]
        provincia = splitted[1]
        if localidad != None and provincia == None and len(locs_data[localidad]) == 1:
            provincia = provs_data[locs_data[localidad][0]]
            splitted[1] = provincia
        return splitted

    @staticmethod
    def getLocationStr(tw, verbose=False):

        try:
            if tw['user'] != None and tw['user']['location'] != None:

                loc = tw['user']['location']

                provincia = ''
                localidad = ''
                abreviatura = ''
                completo = ''
                loc = translate(loc.lower())

                # Quito varios espacios por 1 solo
                loc = re.sub('\s+', ' ', loc)
                localidadComp = loc.strip()
                transformacion = localidadComp

                for p in provs:
                    if p in loc:
                        provincia = p
                        break

                if provincia != '':
                    loc = re.sub(provincia, '', loc)
                transformacion += ' - ' + loc

                for l in locs:
                    if l in loc:
                        if len(l) > len(localidad):
                            localidad = l
                if localidad != '':
                    loc = re.sub(localidad, '', loc)

                transformacion += ' - ' + loc

                for a in abreviaturas:
                    if a in loc:
                        abreviatura = a
                        break

                for c in completos:
                    if c == localidadComp:
                        completo = c
                        break

                if (provincia != '') and (localidad != '') and 'argentina' in loc:
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado full --> ', localidad, ',', provincia, ', argentina'
                        print
                        print
                    return ('full',
                            # localidad + ',' + provincia + ', argentina',
                            [localidad, provincia, 'argentina'])

                elif (provincia != '') and (localidad != ''):
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado prov. & loc. --> ', localidad, ',', provincia
                        print
                        print
                    return ('provAndLoc',
                            # localidad + ',' + provincia,
                            [localidad, provincia, None, None])

                elif ((provincia != '') or (localidad != '')) and 'argentina' in loc:
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado definido con pais--> ', localidad, ',', provincia, ', argentina'
                        print
                        print
                    return ('paisDefined',
                            # localidad + provincia + ', argentina',
                            [localidad if localidad != '' else None, provincia if provincia != '' else None,
                             'argentina', None])


                elif completo != '':
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado completo --> ', completo
                        print
                        print
                    return ('encCompletos',
                            # completo,
                            [None, None, None, completo])

                elif ((provincia != '') or (localidad != '')) and abreviatura != '':
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado definido con extra--> ', localidad, ',', provincia, ',', abreviatura
                        print
                        print
                    return ('extraDefined',
                            # localidad + provincia + ',' + abreviatura,
                            [localidad, provincia, None, abreviatura])

                elif ((provincia == '') and (localidad == '')) and abreviatura != '':
                    if verbose:
                        print localidadComp
                        print transformacion
                        print 'Econtrado breviatura--> ', localidad, ',', provincia, ',', abreviatura
                        print
                        print
                    return ('encAbreviaturas',
                            # abreviatura,
                            [None, None, None, abreviatura])

            return (None, None)
        except Exception as e:
            # print e
            # print tw
            return (None, None)

    def __getGeocodeLugar(self, lugar):
        result = self.__gmaps.geocode(lugar)

        if len(result) > 0:
            return result[0]['geometry']['location']
        else:
            print '## NO ENCONTRADO ## -->', lugar

    def findLocation(self, tw):
        try:
            if 'coordinates' in tw and tw['coordinates'] != None:
                lng = tw["coordinates"][u"coordinates"][0]
                lat = tw["coordinates"][u"coordinates"][1]
                if self.__geocodesCount > 0 and self.__geocodesCount % 24 == 0:
                    time.sleep(1)
                gmaps_location = self.__gmaps.reverse_geocode((lat, lng))
                self.__geocodesCount += 1

                localidad = ''
                provincia = ''
                pais = ''

                for g_location in gmaps_location:
                    if 'locality' in g_location['types']:
                        for addr_comp in g_location['address_components']:
                            if 'locality' in addr_comp['types']:
                                localidad = addr_comp['long_name'].lower()
                                localidad = translate(localidad)
                            elif 'administrative_area_level_1' in addr_comp['types']:
                                provincia = re.sub('Province', '', addr_comp['long_name']).strip().lower()
                                provincia = translate(provincia)
                            elif 'country' in addr_comp['types']:
                                pais = addr_comp['long_name'].lower()
                                pais = translate(pais)
                        if pais == 'argentina':
                            formatted_address = localidad + ',' + provincia + ',' + pais
                            # Si no existe el lugar en el archivo locations.json lo agrego
                            if (formatted_address not in self.__locations):
                                self.__locations[formatted_address] = g_location['geometry']['location']

                            splitted = formatted_address.split(',')
                            splitted.append(None)
                            res = ({'lat': lat, 'lng': lng}, formatted_address, splitted)
                            print '######## Encontrado por coordenadas #####################'
                            print res
                            print '###############################'
                            return res
            # Si a traves del tag 'coordinates' no pudo encontrar la direccion 'localidad,provincia,pais'

            tipo, splitted = TweetLocations.getLocationStr(tw, verbose=False)

            if tipo != None:
                # Me fijo si puedo determinar la provincia de las localidades
                # que se identificaron por estar 'argentina' en el campo, ej: trelew, argentina --> trelew,chubut,argentina
                # siempre y cuando la localidad exista en una unica provincia
                if tipo == 'paisDefined':
                    splitted = TweetLocations.completarProvincia(splitted)

                lugar = TweetLocations.locationMerge(splitted)
                if (tipo not in self.__tiposNoPermitidos) and (lugar not in self.__locations):
                    if self.__geocodesCount > 0 and self.__geocodesCount % 24 == 0:
                        time.sleep(1)
                    self.__locations[lugar] = self.__getGeocodeLugar(lugar)
                    self.__geocodesCount += 1
                    print '###### Encontrado por campo location ########'
                    print lugar, ' - coordenadas: ', self.__locations[lugar]
                    print '#############################################'

                if lugar in self.__locations:
                    return (self.__locations[lugar], lugar, splitted)

            return None
        except Exception as e:
            print e
            print tw
            return None

    def saveLocations(self):
        if self.__geocodesCount > 0:
            locations_file = codecs.open(os.path.dirname(__file__) + '/locations.json', mode='w', encoding='utf-8')
            json.dump(self.__locations, locations_file)
