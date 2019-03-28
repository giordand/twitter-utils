import os,logging
INDEX_PATH=os.path.dirname(__file__)+'/tweets_content_processor/index/'
USERS_TIMELINE_PATH=os.path.dirname(__file__)+'/tweets_content_processor/users_timeline/'
USERS_TEXTSIM_FILE=os.path.dirname(__file__)+'/tweets_content_processor/users_sim.csv'
USERS_DOCS_FILE=os.path.dirname(__file__)+'/tweets_content_processor/users_docs.json'
CONCAT_USERS_DOCS_FILE=os.path.dirname(__file__)+'/tweets_content_processor/CONCAT_users_docs.json'
LOGS_PATH=os.path.dirname(__file__)+'/logs/'

CELIAQUIA_URL = 'mongodb://192.168.0.5:27017'
CELIAQUIA_DATABASE = 'get_my_tweet_back'
CELIAQUIA_COLLECTION = 'tweets'

def getLog(name,filename):

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.INFO)


    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh.setFormatter(formatter)
    # add the handlers to logger

    logger.addHandler(fh)
    return logger
