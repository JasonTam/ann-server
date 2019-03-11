import json
from time import time
import s3fs
from annoy import AnnoyIndex
from pathlib import Path


METRIC = 'angular'  # TODO: should come from some config
FACTORS_KEY = 'factors'
ID_KEY = 'id'
N_TREES = 10
PATH_DISK_SAVE = '/tmp/index.ann'

# PATH_ANN = 's3://jason-garbage/ann/glove1m/glove1m.ann'
# PATH_IDS = 's3://jason-garbage/ann/glove1m/glove1m_ids'

PATH_ANN = 's3://jason-garbage/ann/glove100/glove100.ann'
PATH_IDS = 's3://jason-garbage/ann/glove100/glove100_ids'

# local_mtime = datetime.datetime.fromtimestamp(os.path.getmtime('/tmp/ann'), tz=datetime.timezone.utc)
# remote_mtime = fs.info('jason-garbage/ann/glove100/glove100.ann')['LastModified']
# local_mtime > remote_mtime


def lambda_handler(event, context):
    print('Whats in tmp')
    print(list(Path('/tmp').glob('*')))

    print('Establishing s3fs connection...')
    fs = s3fs.S3FileSystem()
    open_fn = fs.open

    print('Downloading ANN...')
    tic = time()
    fs.get(PATH_ANN, '/tmp/ann')
    toc = time() - tic
    print(f'...{toc}s')

    print('Loading ANN...')
    ann = AnnoyIndex(100)
    ann.load('/tmp/ann')

    print('Loading Ids...')
    tic = time()
    ids = [s.decode('utf-8') for s in
           open_fn(PATH_IDS, 'rb').read().splitlines()]
    ids_d = dict(zip(ids, range(len(ids))))
    toc = time() - tic
    print(f'...{toc}s')

    q = ann.get_nns_by_item(10, 4)

    print('Printing to stdout')
    return {
        'statusCode': 200,
        'body': json.dumps(q)
    }
