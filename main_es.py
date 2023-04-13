from ConfigProcessor import ConfigProcessor
import pandas as pd
import logging, sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.helpers import BulkIndexError



def create_document(index, doc_id, row):
    doc = {
        '_index': index,
        '_id': doc_id
    }
    for col in row.index:
        doc[col] = row[col]
    return doc


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger = logging.getLogger('ES_log')
logger.setLevel(logging.INFO)

with ConfigProcessor('ES.ini') as config:
    filename = config.get_option('DETAILS', 'filename')
    output_log = config.get_option("DETAILS", 'log')
    db_hostname = 'http://'+config.get_option('DETAILS', 'db_hostname')+'/'

    if output_log == 'True':
        logger.addHandler(stdout_handler)

    df_test = pd.read_csv(filename, sep='|', engine='python')
    logger.info("The db hostname is: {}".format(db_hostname))
    logger.info("Reading the file with the filename: {}".format(filename))
    file_length = df_test.shape[0]
    logger.info("The number of records in the file are {}".format(file_length))

    index_name = filename.split('_')[0].lower()
    es = Elasticsearch(db_hostname, request_timeout=30)
    docs = []
    start = 0
    while start < file_length:
        if (file_length-start) < 1000:
            batch_size = file_length-start
        else:
            batch_size=1000
        logger.info("Processing {} number of records".format(batch_size))
        for i, row in df_test[start:start+batch_size].iterrows():
            doc = create_document(index_name, i, row)
            docs.append(doc)

        try:
            success, failed = bulk(es, docs)
        except BulkIndexError as e:
            print(f"Number of documents failed to index: {len(e.errors)}")
            for error in e.errors:
                print(error)

        logger.info(f"Number of successful uploads: {success}")
        start = start + 1000
