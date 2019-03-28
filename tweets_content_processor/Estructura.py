import ModeloDatos as md
import numpy

# Esta clase tiene el comportamiento necesario para llevar a cabo las
# estadisticas del procesamiento del corpus
# (cantidad de tokens,cantidad de terminos, promedio del largo de un termino,etc)
class Estructura(object):

    def __init__(self):
        self.__maxLenDocName = 0

        # Estructura del vocabulario = 'termino': [DF,P_Offset,idf]
        self.__vocabulario = md.getVocabulario()

        # Estructura = 'doc_index': [filename,Norma_NormMax]
        self.__users = md.getUsuarios()

        self.__users_by_username = {}
        for u_id,data in self.__users.iteritems():
            self.__users_by_username[data[0]] = data[1:]

        # Calculo IDF para cada termino
        for t,datos in self.__vocabulario.iteritems():
            self.__vocabulario[t].append(float(numpy.log10(len(self.__users) / float(datos[0]))))

        # Estructura indice invertido = 'id_term': [[id_doc,frecuencia]]
        self.__invertedIndex = {}

    def isInVocabulario(self,termino):
        return termino in self.__vocabulario

    '''Devuelve la posting del termino que se pasa como parametro. Si la posting no se encuentra
    en memoria se va a buscar a disco'''
    def getPosting(self,termino):

        # Si el termino esta en el vocabulario
        if self.__vocabulario.has_key(termino):

            # Si el termino no esta en el indice invertido
            if termino not in self.__invertedIndex:
                # Obtengo los datos del termino
                datos =  self.__vocabulario[termino]
                # Le pido al modelo de datos la posting del termino
                p = md.getPostings(datos[0], datos[1])
                # Si se encontraron las postings
                if p != None:
                    # Cargo la posting en el II que tengo en memoria
                    self.__invertedIndex[termino] = p
                # Devuelvo el resultado de la busqueda
                return p
            else:
                return self.__invertedIndex[termino]
        # Si el termino no esta en el vocabulario
        else:
            return None

    def getVocabulario(self):
        return self.__vocabulario

    def getIndiceInvertido(self):
        for t in self.__vocabulario.iterkeys():
            self.getPosting(t)
        return self.__invertedIndex

    def getDocumentosProcesados(self):
        return self.__users

    def getCantidadTerminos(self):
        return len(md.getVocabulario())

    def getIdfTermino(self,t):
        if t in self.getVocabulario():
            return self.getVocabulario()[t][2]

    def getMaxFreqUser(self,u):
        if u in self.getDocumentosProcesados():
            return float(self.getDocumentosProcesados()[u][1])

    def getCosenoNormMax(self,u_id):
        if u_id in self.getDocumentosProcesados():
            return float(self.getDocumentosProcesados()[u_id][2])

    def getCosenoNormMaxUN(self,user_name):
        if user_name in self.__users_by_username:
            return float(self.__users_by_username[user_name][1])


