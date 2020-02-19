from annoy import AnnoyIndex
import json
from time import time
from typing import Dict, List, Tuple, Union, Optional
import boto3
from botocore.exceptions import ClientError
import s3fs
import datetime
import tarfile
import struct
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)

S3_URI_PREFIX = 's3://'


PATH_TMP = Path('/tmp/ann')
PATH_TMP.parent.mkdir(parents=True, exist_ok=True)
ANN_INDEX_KEY = 'index.ann'
ANN_IDS_KEY = 'ids.txt'
ANN_META_KEY = 'metadata.json'
TIMESTAMP_LOCAL_KEY = 'timestamp.txt'
DYNAMO_ID = 'variant_id'
DYNAMO_KEY = 'repr'
DTYPE_FMT = 'f'  # float32 struct
DTYPE_SZ = 4  # float32 is 4 bytes
SEED = 322

PathType = Union[Path, str]

s3 = s3fs.S3FileSystem()
dynamodb = boto3.resource('dynamodb')


def is_s3_path(path: PathType):
    return str(path).startswith(S3_URI_PREFIX)


def load_fallback_map(path_fallback_map: PathType) -> Dict[str, str]:
    if is_s3_path(path_fallback_map):
        open_fn = s3.open
    else:
        open_fn = open

    with open_fn(path_fallback_map, 'rb') as f:
        fallback_map = json.load(f)

    return fallback_map


def needs_reload(path_tar: PathType,
                 ts_read_utc: Optional[datetime.datetime],
                 ) -> bool:
    if ts_read_utc is None:
        # no timestamp file means index is not downloaded/extracted yet
        return True
    else:
        # Container re-use: check if new index exists on remote
        s3.invalidate_cache(path_tar)
        remote_mtime = s3.info(path_tar)['LastModified']

        return remote_mtime > ts_read_utc


def load_via_tar(path_tar: PathType,
                 path_extract: PathType,
                 reload: bool = True):
    path_local_ts_read = path_extract / TIMESTAMP_LOCAL_KEY
    if reload:
        # Write to timestamp file
        # Note: `fromisoformat` only in Py3.7
        # ts_read = datetime.datetime.utcnow().isoformat()
        ts_read = int(time())
        ann_tar = tarfile.open(fileobj=s3.open(path_tar, 'rb'))
        ann_tar.extractall(path_extract)

        with open(path_local_ts_read, 'w') as f:
            f.write(str(ts_read))

    meta_d = load_ann_meta(path_extract / ANN_META_KEY)
    ann_ids, ann_ids_d = load_ids(path_extract / ANN_IDS_KEY)
    ann_index_path = path_extract / ANN_INDEX_KEY

    return ann_index_path, ann_ids, ann_ids_d, path_local_ts_read, meta_d


def load_ann_meta(path_meta: PathType) -> Dict:
    meta_d = json.load(open(path_meta, 'r'))
    return meta_d


def load_index(path_index: PathType,
               meta_d: Dict) \
        -> AnnoyIndex:
    """ We rely on ANNOY's usage of mmap to be fast loading
    (fast enough that we can load it on every single call)
    """
    n_dim = meta_d['n_dim']
    metric = meta_d['metric']
    u = AnnoyIndex(
        n_dim,
        metric=metric,
    )
    u.load(str(path_index))
    u.set_seed(SEED)
    return u


def load_ids(path_ids: PathType) -> Tuple[List[str], Dict[str, int]]:
    if is_s3_path(path_ids):
        open_fn = s3.open
    else:
        open_fn = open

    ids = [s.decode('utf-8') for s in
           open_fn(path_ids, 'rb').read().splitlines()]
    ids_d = dict(zip(ids, range(len(ids))))
    return ids, ids_d


def get_dynamo_emb(table,
                   variant_id,
                   id_key=DYNAMO_ID, repr_key=DYNAMO_KEY):
    try:
        response = table.get_item(Key={id_key: variant_id})
        item = response['Item']
    except (ClientError, KeyError) as e:
        return None
    else:
        b_str = item[repr_key].value
        emb = struct.unpack(DTYPE_FMT * (len(b_str) // DTYPE_SZ), b_str)
    return emb
