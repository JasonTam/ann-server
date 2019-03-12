import json
import fastavro as avro
import s3fs
from annoy import AnnoyIndex
from pathlib import Path
from datetime import datetime
import tarfile
from io import BytesIO, StringIO


METRIC = 'angular'  # TODO: should come from some config
FACTORS_KEY = 'factors'
ID_KEY = 'id'
N_TREES = 10
PATH_DISK_SAVE = '/tmp/index.ann'


def lambda_handler(event, context):
    # `s3:ObjectCreated:Put` can only ever create 1 record. Take head.
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = Path(event['Records'][0]['s3']['object']['key'])
    key_name = key.name  # ex) 'part.avro'
    key_base = key.stem
    path_data = f's3://{bucket}/{key}'
    print(f'Bucket: {bucket}')
    print(f'Key: {key}', '\t', f'Key name: {key_name}')
    print(f'Path: {path_data}')

    print('Establishing s3fs connection...')
    fs = s3fs.S3FileSystem()
    open_fn = fs.open

    print('Streaming in vectors...')
    ts_read = datetime.utcnow().isoformat()
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

    print('Prep Metadata')
    meta_d = {
        'vec_src': path_data,
        'metric': METRIC,
        'n_dim': n_dim,
        'timestamp_utc': ts_read,
    }

    print('Exporting files...')

    # Create tarball buffer
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tar_buf:
        # Add index
        tar_buf.add(PATH_DISK_SAVE, arcname='index.ann')

        # Add ids  TODO: will be lmdb
        info = tarfile.TarInfo(name='ids.txt')
        ids_bytes = '\n'.join(ids).encode('utf-8')
        ids_buf = BytesIO(ids_bytes)
        info.size = len(ids_bytes)
        tar_buf.addfile(tarinfo=info, fileobj=ids_buf)

        # Add Metadata
        info = tarfile.TarInfo(name='metadata.json')
        meta_bytes = json.dumps(meta_d).encode('utf-8')
        meta_buf = BytesIO(meta_bytes)
        info.size = len(meta_bytes)
        tar_buf.addfile(tarinfo=info, fileobj=meta_buf)

    buf.seek(0)
    with open_fn(f's3://{bucket}/ann/{key_base}.tar', 'wb') as fo:
        fo.write(buf.read())

    print('Printing to stdout')
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
