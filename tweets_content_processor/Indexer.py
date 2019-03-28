# _*_coding:utf-8_*_

from TwitterContent import readUsersTimeline,getDumpsNumber
from TwitterText import stopwords,tweetsAsTokenizedDoc
import numpy,logging
import ModeloDatos as md
from ..configurations import LOGS_PATH,getLog

logger = getLog(__name__,LOGS_PATH + __name__ + '.log')

def aumentarVocabulario(voc, tokens):
    for t in tokens:
        if t in voc:
            voc[t] += 1
        else:
            voc[t] = 1

class TweetsIndexer(object):

    def __init__(self):
        self.__stopwords = stopwords()
        # Estructura del vocabulario = 'termino': [DF,idf]
        self.__vocabulario = {}
        # Estructura = 'doc_index': [filename,MaxTF,Norma_NormMax]
        self.__usuarios = {}

        self.__usersVocab = {}

        self.__invertedIndex = {}
        self.__docIdC = 1
        self.__cantTermProc = 1
        self.__unicos = 0
        self.__proc_num = 10
        self.__users_indexed = 1

    def run(self):
        import time
        users_indexed = 1
        nDumps = getDumpsNumber()

        logger.log(logging.INFO, 'Cantidad bloques timeline =' + str(nDumps))
        for i in xrange(1,nDumps+1):
            timeline = readUsersTimeline(i)

            logger.log(logging.INFO, 'Procesando timeline =' + str(i))
            for u,tweets in timeline.iteritems():
                if tweets!=None:

                    logger.log(logging.INFO, 'Indexando usuario ' + u + ', cantidad de tweets = '+str(len(tweets)))
                    start = time.time()
                    lista_terminos = tweetsAsTokenizedDoc(tweets, avoidRT=True, removeStops=True)

                    if len(lista_terminos)>0:
                        # Actualizo las estadisticas del documento mas largo y mas corto
                        self.__actualizarUserStats(u, lista_terminos)
                        self.__usersVocab[u] = list(set(lista_terminos))

                        logger.log(logging.INFO, 'Tiempo ' + str(time.time()-start))
                        users_indexed += 1


            logger.log(logging.INFO, 'Usuarios indexados =' + str(users_indexed-1))
        self.__computeNorm()

        # Guardo los archivos
        md.guardarIndice(self.__invertedIndex)
        md.guardarUsuarios(self.__usuarios)
        md.saveUsersVocab(self.__usersVocab)

    def __actualizarUserStats(self, user, terminos):

        # Agrego los terminos al vocabulario y le doy un id
        for t in terminos:
            if not t in self.__vocabulario:
                self.__vocabulario[t] = []

        # Agrego el usuario a los procesados y le asigno un id
        docIndex = self.__nextIdDoc()
        self.__usuarios[docIndex] = [user, 0]

        tmp = {}
        for t in terminos:
            if t in tmp:
                tmp[t] += 1
            else:
                tmp[t] = 1

        fMax = 0
        for t in tmp:
            if not t in self.__invertedIndex:
                self.__invertedIndex[t] = [[docIndex, tmp[t]]]
            else:
                self.__invertedIndex[t].append([docIndex, tmp[t]])
            if fMax < tmp[t]:
                fMax = tmp[t]

        # Guardo la maxima frecuencia registrada para el documento
        self.__usuarios[docIndex][1] = fMax

    '''Calculo la norma para cada documento. Una para TF-IDF con TF normalizado por NormMax y otra para TF normalizado por la formula
        de Robertson'''

    def __computeNorm(self):

        # Calculo IDF para cada termino
        for t, docs in self.__invertedIndex.iteritems():
            self.__vocabulario[t].append(float(numpy.log10(len(self.__usuarios) / float(len(docs)))))

        norma_docs = {}
        for t, docs in self.__invertedIndex.iteritems():
            # Por cada documento
            for doc in docs:
                # NormMax
                tf_idf_nm = (doc[1] / float(self.__usuarios[doc[0]][1])) * self.__vocabulario[t][0]
                # Robertson
                tf_idf_rb = (doc[1] / float(doc[1] + 1)) * self.__vocabulario[t][0]

                # Acumulo el valor del producto punto para el documento que procese
                if doc[0] in norma_docs:
                    norma_docs[doc[0]][0] += pow(tf_idf_nm, 2)
                    norma_docs[doc[0]][1] += pow(tf_idf_rb, 2)
                else:
                    norma_docs[doc[0]] = [pow(tf_idf_nm, 2), pow(tf_idf_rb, 2)]

        for d, sum_cuad in norma_docs.iteritems():
            # Calculo la raiz para NormMax
            self.__usuarios[d].append(numpy.sqrt(sum_cuad[0]))
            # Calculo la raiz para Robertson
            self.__usuarios[d].append(numpy.sqrt(sum_cuad[1]))
        return True


    def __nextIdDoc(self):
            self.__docIdC += 1
            return self.__docIdC - 1
