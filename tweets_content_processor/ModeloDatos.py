# _*_coding:utf-8_*_

import struct,json
from bson.json_util import dumps
import codecs
from ..configurations import INDEX_PATH

LEN_TERM = 20  # En caracteres
LEN_DF = 4  # En bytes
LEN_P_OFF = 4  # En bytes
LEN_DOC_NAME = 150  # En caracteres
LEN_ID_DOC = 4  # En bytes


POSTING_FILE = INDEX_PATH+'postings'
VOCAB_FILE = INDEX_PATH+'vocabulario'
USERS_FILE = INDEX_PATH+'usuarios'
USERS_VOCAB_FILE = INDEX_PATH + 'users_dict'

TERM_LEN_BYTES = 100

def guardarIndice(indice):

    try:
        with codecs.open(POSTING_FILE, 'wb') as p, \
                codecs.open(VOCAB_FILE, 'wb') as v:
                # codecs.open(VOCAB_FILE, 'w', encoding='utf-8') as v:
            # Inicio el offset del archivo de postings en 0
            offP = 0

            for t, datos in sorted(indice.iteritems(), key=lambda x: x[0]):
                postings = [x[0] for x in datos]
                freqs = [x[1] for x in datos]

                # Genero d-gaps
                postings = dgapsTrans(postings)

                x = 0
                while len(postings) > x:
                    # Transformo los datos a binario
                    doc = struct.pack('I', postings[x])
                    freq = struct.pack('I', freqs[x])
                    # Escribo las postings --> iddoc1 | freq | p_posiciones
                    p.write(doc)
                    p.write(freq)
                    x += 1

                # Escribo la entrada en el vocabulario --> termino | DF | puntero_inicio_postings

                df = str(len(postings))

                #v.write(t + ',' + df + ',' + str(offP) + '\n')


                encoded = t.encode('utf-8')
                encoded = encoded.ljust(TERM_LEN_BYTES,'\0')
                v.write(encoded)
                v.write(struct.pack('I',len(postings)))
                v.write(struct.pack('I', offP))

                # Actualizo el puntero de las postings
                offP += len(postings) * (4 + LEN_ID_DOC)

    except IOError as e:
        print e


def guardarUsuarios(documentos):
    try:
        with codecs.open(USERS_FILE, 'w', encoding='utf-8') as d:
            for id_doc, datos in sorted(documentos.iteritems(), key=lambda x: x[0]):
                # Guardo id_doc,MaxFreq,norma_NormMax,norma_Robertson, usuario

                d.write(str(id_doc) + ',' + str(datos[1]) + ',' + str(datos[2]) + ',' + str(datos[3]) + ',' + datos[0] + '\n')
    except IOError as e:
        print e

def saveUsersVocab(usersVocab):
    try:
        with codecs.open(USERS_VOCAB_FILE, 'w', encoding='utf-8') as ud:
            ud.write(dumps(usersVocab))
    except IOError as e:
        print e

def loadUsersVocab():
    try:
        with codecs.open(USERS_VOCAB_FILE, mode='r', encoding='utf-8') as ud:
            return json.load(ud)
    except IOError as e:
        print e

def getVocabulario():
    vocabulario = {}
    # try:
    #     with codecs.open(VOCAB_FILE, 'r', encoding='utf-8') as v:
    #         for l in v.readlines():
    #             datos = l.split(',')
    #             vocabulario[datos[0].strip()] = [int(datos[1]), int(datos[2])]
    # except IOError as e:
    #     print e
    # return vocabulario

    try:
        with codecs.open(VOCAB_FILE, 'rb') as v:
            off = 0
            while v.read(1) != '':
                v.seek(off)
                term = v.read(TERM_LEN_BYTES).rstrip('\x00').decode('utf-8')
                df = struct.unpack('I', v.read(4))[0]
                offsetP = struct.unpack('I', v.read(4))[0]
                vocabulario[term.strip()] = [df, offsetP]
                off += TERM_LEN_BYTES + 4 + 4
    except IOError as e:
        print e
    return vocabulario


def getPostings(df, offset):
    posting = []
    freqs = []
    try:
        with codecs.open(POSTING_FILE, 'rb') as v:
            # Mientras no sea fin de archivo
            v.seek(offset)
            x = 0
            while x < df:
                if v.read(1) != '':
                    v.seek(offset)
                    doc = struct.unpack('I', v.read(4))[0]
                    freq = struct.unpack('I', v.read(4))[0]

                    posting.append(doc)
                    freqs.append(freq)

                    offset += 8
                x += 1
    except IOError as e:
        print e
    posting = postingsTrans(posting)
    j = 0
    res = []
    while j < len(posting):
        res.append((posting[j], freqs[j]))
        j += 1
    return res


def getUsuarios():
    # Estructura = {'doc_id': [doc_name,MaxFreq,Norma-NormMax,Norma-Robertson]}
    docs = {}
    try:
        with codecs.open(USERS_FILE, 'r', encoding='utf-8') as v:
            for line in v.readlines():
                ls = line.split(',')
                newLs = [int(ls[0]), int(ls[1]), float(ls[2]), ls[3], ls[4].rstrip(u'\n')]
                if len(ls) > 5:
                    x = 5
                    while x < len(ls):
                        newLs[4] += ',' + ls[x]
                        x += 1
                    newLs[4] = newLs[4].rstrip(u'\n')

                docs[newLs[0]] = [newLs[4], newLs[1], newLs[2], newLs[3]]
    except IOError as e:
        print e
    return docs


def dgapsTrans(docs):
    docs = sorted(docs)
    suma = 0
    dg = [docs[0]]
    suma = docs[0]
    x = 1
    while x < len(docs):
        dg.append(docs[x] - suma)
        suma = docs[x]
        x += 1
    return dg


def postingsTrans(dgaps):
    posts = [dgaps[0]]
    suma = dgaps[0]
    x = 1
    while x < len(dgaps):
        posts.append(dgaps[x] + suma)
        suma += dgaps[x]
        x += 1
    return posts