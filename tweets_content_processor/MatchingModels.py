#!/usr/bin/env python
# _*_coding:utf-8_*_

# refer to http://staff.science.uva.nl/~tsagias/?p=185
import re, math, collections, sys
from Estructura import Estructura

index_ondsk = Estructura()

def tokenize(_str):
    stopwords = ['and', 'for', 'if', 'the', 'then', 'be', 'is', 'are', 'will', 'in', 'it', 'to', 'that']
    tokens = collections.defaultdict(lambda: 0.)
    for m in re.finditer(r"(\w+)", _str, re.UNICODE):
        m = m.group(1).lower()
        if len(m) < 2: continue
        if m in stopwords: continue
        tokens[m] += 1

    return tokens


# end of tokenize

def kldiv(_s, _t):
    if (len(_s) == 0):
        return 1e33

    if (len(_t) == 0):
        return 1e33

    ssum = 0. + sum(_s.values())
    slen = len(_s)

    tsum = 0. + sum(_t.values())
    tlen = len(_t)

    vocabdiff = set(_s.keys()).difference(set(_t.keys()))
    lenvocabdiff = len(vocabdiff)

    """ epsilon """
    epsilon = min(min(_s.values()) / ssum, min(_t.values()) / tsum) * 0.001

    """ gamma """
    gamma = 1 - lenvocabdiff * epsilon

    # print "_s: %s" % _s
    # print "_t: %s" % _t

    """ Check if distribution probabilities sum to 1"""
    sc = sum([v / ssum for v in _s.itervalues()])
    st = sum([v / tsum for v in _t.itervalues()])

    if sc < 9e-6:
        print "Sum P: %e, Sum Q: %e" % (sc, st)
        print "*** ERROR: sc does not sum up to 1. Bailing out .."
        sys.exit(2)
    if st < 9e-6:
        print "Sum P: %e, Sum Q: %e" % (sc, st)
        print "*** ERROR: st does not sum up to 1. Bailing out .."
        sys.exit(2)

    div = 0.
    for t, v in _s.iteritems():
        pts = v / ssum

        ptt = epsilon
        if t in _t:
            ptt = gamma * (_t[t] / tsum)

        ckl = (pts - ptt) * math.log(pts / ptt)

        div += ckl

    return div


def JelinekMercer(doc,colection,docPond=.7):
    '''

    :param doc: diccionario con termino:df
    :type doc: dict
    :param colection: diccionario con termino:cf
    :type colection: dict
    :param docPond: ponderacion del suavizado para el cocumento
    :type docPond: float
    :return: modelo de lenguaje del documento
    '''
    res = {}
    cantTerminosCol = sum([a for a in colection.itervalues()])
    cantTerminosDoc = sum([a for a in doc.itervalues()])
    colPond = 1.0-docPond
    for t,cf in colection.iteritems():
        df = 0
        if doc.has_key(t):
            df = doc[t]
        res[t] = (docPond*(df/float(cantTerminosDoc)))+(colPond*(cf/float(cantTerminosCol)))
    return res


def KullbackLeiblerDivergence(p, q):
    '''

    :param p: distribucion de terminos 1
    :type p: dict
    :param q: distribucion de terminos 2
    :type q: dict
    :return: valor de divergencia KL
    '''
    div = 0
    for t,prob in p.iteritems():
        if not q.has_key(t) or (q.has_key(t) and q[t]==0):
            raise Exception('La probabilidad para '+t+'no puede ser 0 en dist2')
        if prob == 0:
            div+=0
        else:
            div+=prob*math.log(prob/q[t])
    return div



def cosineSimilarity(p, q, up, uq):
    '''
    Calcula la similitud entre dos usuarios.
    El calculo se basa en la formula presentada en el libro 'Information retrieval' Christopher D. Manning,
    donde, en vez de calcularse la similud de un
    :param p: Diccionario {'termino':frecuencia} del documento formado con todos los tweets
    del usuario 'p'
    :type p: dict
    :param q: Diccionario {'termino':frecuencia} del documento formado con todos los tweets
    del usuario 'q'
    :type q: dict
    :param up: screen_name del usuario p
    :type up: str
    :param uq: screen_name del usuario q
    :type uq: str
    :return: float
    '''

    p_max_freq = max(p.values())
    q_max_freq = max(q.values())
    p_tfidf = {}
    q_tfidf = {}

    # Calculo el valor del TF-IDF para los terminos del usuario 'p'
    # segun la formula de Baeza --> (Freq(t)/MaxFreq(p)) * Idf t
    for t, f in p.iteritems():
        p_tfidf[t] = (f / float(p_max_freq)) * index_ondsk.getIdfTermino(t)

    # Calculo el valor del TF-IDF para los terminos del usuario 'q'
    # segun la formula de Baeza --> (Freq(t)/MaxFreq(q)) * Idf t
    for t, f in q.iteritems():
        q_tfidf[t] = (f / float(q_max_freq)) * index_ondsk.getIdfTermino(t)


    # Se calcula el producto punto entre los 'vectores' de pesos de los terminos de los usuarios
    # Busco el usuarios con menor terminos para hacer un producto punto mas eficiente
    if len(p.keys()) <= len(q.keys()):
        iter = p_tfidf
        comp = q_tfidf
    else:
        iter = q_tfidf
        comp = p_tfidf

    # Calculo el producto punto
    producto = 0
    for t,w in iter.iteritems():
        if t in comp:
            # Se le suma al producto el peso del termino 't' en el usuario 1 por el peso en el usuario 2.
            producto += w*comp[t]

    # Finalmente dividimos por el coseno de ambos usuarios
    producto = producto / float(index_ondsk.getCosenoNormMaxUN(up)*index_ondsk.getCosenoNormMaxUN(uq))

    return producto


