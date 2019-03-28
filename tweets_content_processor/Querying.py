# _*_coding:utf-8_*_

from Estructura import Estructura

class Querying(object):

    def __init__(self, rankingMethod):

        self.__e = Estructura()

        self.__rankingMethod = []

        if rankingMethod == 'NormMax':
            self.__rankingMethod = ['Coseno', 'NormMax']
        elif rankingMethod == 'Robertson':
            self.__rankingMethod = ['Coseno', 'Robertson']
        else:
            self.__rankingMethod = ['NormMax']


    def getDocumentosProcesados(self):
        return self.__e.getDocumentosProcesados()

    def getVocabulario(self):
        return self.__e.getVocabulario()

    def queringVSM(self, user, query):
        '''

        :param query:
        :type query: str
        :return:
        '''
        res = []
        # Con la clase Procesador obtengo los tokens
        lista_tokens_q = query.split(' ')

        # Sigo analizando en caso de que al menos haya un token en el query
        if (len(lista_tokens_q) > 0):
            return self.__getRankingVSM(user,lista_tokens_q)

        return res

    def __getRankingVSM(self,user, terminos_q):

        # Genero el diccionario del query, quitando aquellos terminos que no
        # se encuentren en el vocabulario. La estructura es 'id_term': Freq (t;q)
        query = {}
        maxFreQ = 0

        for t in terminos_q:
            if self.__e.isInVocabulario(t):
                if t in query:
                    query[t] += 1
                else:
                    query[t] = 1
                if query[t] > maxFreQ:
                    maxFreQ = query[t]


        # Uso NormMax para que el TF_IDF sea el mismo para el documento y el query
        for t, f in query.iteritems():
            query[t] = (query[t] / maxFreQ) * self.__e.getVocabulario()[t][2]

        # Estructura que contiene 'doc_id':producto_punto
        res_parcial = {}

        # Por cada termino del query
        for t, wtq in query.iteritems():

            # Obtengo los documentos donde aparece el termino
            docs = self.__e.getPosting(t)
            # Por cada documento
            for doc in docs:
                tf_idf = 0

                if 'NormMax' in self.__rankingMethod:
                    # Calculo el valor del TF-IDF segun la formula de Baeza --> (Freq(i;j)/MaxFreq(t;j)) * Idf t
                    tf_idf = (doc[1] / float(self.__e.getDocumentosProcesados()[doc[0]][1])) * self.__e.getVocabulario()[t][2]
                elif 'Robertson' in self.__rankingMethod:
                    # Calculo el valor del TF-IDF, con TF segun robertson --> (Freq(i;j)/(Freq(i;j)+1)) * Idf t
                    tf_idf = (doc[1] / float(doc[1]+1)) * self.__e.getVocabulario()[t][2]


                # Acumulo el valor del producto punto para el documento que procese
                if doc[0] in res_parcial:
                    res_parcial[doc[0]] += tf_idf * wtq
                else:
                    res_parcial[doc[0]] = tf_idf * wtq

        # Genero una estructura de respuesta= [(nombre_documento,score)]. Donde el score estara dado por
        # el metodo del coseno; aqui no se toma en cuenta la norma del query dado que es una constante y
        # no hace variar el ranking final
        ranking = []
        for doc, dot_prod in res_parcial.iteritems():
            doc_dat = self.__e.getDocumentosProcesados()[doc]
            if 'Coseno' in self.__rankingMethod:
                if 'NormMax' in self.__rankingMethod:
                    if float(doc_dat[2]) > 0:
                        ranking.append((doc_dat[0], dot_prod / float(doc_dat[2])))
                    else:
                        ranking.append((doc_dat[0], 0))
                elif 'Robertson' in self.__rankingMethod:
                    ranking.append((doc_dat[0], dot_prod / float(doc_dat[3])))
            else:
                ranking.append((doc_dat[0], dot_prod))

        # Devuelvo el ranking ordenado por valor del score
        return sorted(ranking, key=lambda x: x[1], reverse=True)


    def getInvertedIndex(self):
        return self.__e.getIndiceInvertido()
