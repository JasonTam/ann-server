import json
import fastavro as avro
import s3fs
from annoy import AnnoyIndex
from pathlib import Path


METRIC = 'angular'  # TODO: should come from some config
FACTORS_KEY = 'factors'
ID_KEY = 'id'
N_TREES = 10
PATH_DISK_SAVE = '/tmp/index.ann'


def lambda_handler(event, context):
    # `s3:ObjectCreated:Put` can only ever create 1 record. Take head.
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key_name = Path(key).name  # ex) 'part.avro'
    path_data = f's3://{bucket}/{key}'
    print(f'Bucket: {bucket}')
    print(f'Key: {key}', '\t', f'Key name: {key_name}')
    print(f'Path: {path_data}')

    print('Establishing s3fs connection...')
    fs = s3fs.S3FileSystem()
    open_fn = fs.open

    print('Streaming in vectors...')
    ids = []
    with open_fn(path_data, 'rb') as f:
        for i, record in enumerate(avro.reader(f)):
            v = record[FACTORS_KEY]
            if i == 0:
                n_dim = len(v)
                ann = AnnoyIndex(n_dim, metric=METRIC)
                ann.on_disk_build(PATH_DISK_SAVE)

            ann.add_item(i, v)
            ids.append(record[ID_KEY])

    print('Building ANN...')
    ann.build(N_TREES)

    print('Exporting files...')
    fs.put(PATH_DISK_SAVE,
           f's3://{bucket}/ann/{key_name}.ann')
    with open_fn(f's3://{bucket}/ann/{key_name}.ids', 'wb') as f:
        f.write('\n'.join(ids).encode('utf-8'))

    print('Printing to stdout')
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
