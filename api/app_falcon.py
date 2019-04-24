import falcon
from annoy import AnnoyIndex
import json
from time import time
from typing import Dict, List, Tuple, Union, Any, Optional
import boto3
from botocore.exceptions import ClientError
import s3fs
import datetime
import tarfile
import struct
import os
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
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
SEED = 322

PathType = Union[Path, str]

s3 = s3fs.S3FileSystem()
dynamodb = boto3.resource('dynamodb')


def is_s3_path(path: PathType):
    return str(path).startswith(S3_URI_PREFIX)


def needs_reload(path_tar: PathType,
                 ts_read_utc: Optional[datetime.datetime],
                 ) -> bool:
    if ts_read_utc is None:
        # no timestamp file means index is not downloaded/extracted yet
        return True
    else:
        # Container re-use: check if new index exists on remote
        remote_mtime = s3.info(path_tar, refresh=True)['LastModified']

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


def load_fallback_map(path_fallback_map: PathType) -> Dict[str, str]:
    if is_s3_path(path_fallback_map):
        open_fn = s3.open
    else:
        open_fn = open

    with open_fn(path_fallback_map, 'rb') as f:
        fallback_map = json.load(f)

    return fallback_map


def get_dynamo_emb(table,
                   d_fmt,
                   variant_id,
                   id_key=DYNAMO_ID, repr_key=DYNAMO_KEY):
    try:
        response = table.get_item(Key={id_key: variant_id})
        item = response['Item']
    except (ClientError, KeyError) as e:
        return None
    else:
        emb = struct.unpack(d_fmt, item[repr_key].value)
    return emb


class ANNResource(object):

    def __init__(self, path_tar: PathType,
                 ooi_table: dynamodb.Table = None,
                 ):
        self.path_tar = path_tar
        self.ooi_table = ooi_table

        # not multithread-safe to do this with multiple indexes per server
        # self.index: AnnoyIndex = None

        self.path_index_local: str = None
        self.ids: List[Any] = None
        self.ids_d: Dict[Any, int] = None
        self.ann_meta_d: Dict[str, Any] = None
        self.fallback_parent: 'ANNResource' = None

        # There is a chance that the ANN is already downloaded in tmp
        self.load(reload=needs_reload(self.path_tar, self.ts_read_utc))

    @property
    def path_extract(self) -> PathType:
        ann_name = Path(self.path_tar).stem.split('.')[0]
        return PATH_TMP / ann_name

    @property
    def ts_read_utc(self) -> Optional[datetime.datetime]:
        path_local_ts_read = self.path_extract / TIMESTAMP_LOCAL_KEY
        if not Path(path_local_ts_read).exists():
            local_mtime = None
        else:
            local_mtime = datetime.datetime.fromtimestamp(
                int(open(path_local_ts_read, 'r').read().strip()),
                tz=datetime.timezone.utc)
        return local_mtime

    @property
    def needs_reload(self):
        return needs_reload(self.path_tar, self.ts_read_utc)

    def load(self, path_tar: str = None, reload: bool = True):
        path_tar = path_tar or self.path_tar
        tic = time()
        logging.info(f'Loading: {path_tar}')
        self.path_index_local, self.ids, self.ids_d, \
            ts_read, self.ann_meta_d = \
            load_via_tar(path_tar, self.path_extract, reload)
        logging.info(f'...Done Loading! [{time() - tic} s]')

    def maybe_reload(self):
        if self.needs_reload:
            logging.info(f'Reloading [{self.path_tar}] due to staleness')
            self.load(reload=True)

    def nn_from_payload(self, payload: Dict):
        # TODO: parse and use `search_k`
        k = payload['k']

        ann_index = load_index(self.path_index_local, self.ann_meta_d)

        q_id = payload['id']

        if q_id in self.ids_d:
            q_ind = self.ids_d[q_id]
            neighbors = [self.ids[ind] for ind in
                         ann_index.get_nns_by_item(q_ind, k + 1)]
        elif self.ooi_table is not None:
            # Need to look up the vector and query by vector
            q_emb = get_dynamo_emb(
                self.ooi_table, self.ann_meta_d['n_dim'] * DTYPE_FMT, q_id)
            if q_emb is None:
                raise Exception(
                    'Q is ooi and doesnt exist in the ooi dynamo table')
            neighbors = [self.ids[ind] for ind in
                         ann_index.get_nns_by_vector(q_emb, k + 1)]
        else:
            # TODO: there's a chance Q is in the fallback parent index
            # TODO: depending on how he indexes were created
            raise Exception('Q is ooi and no ooi dynamo table was set')

        if q_id in neighbors:
            neighbors.remove(q_id)

        # Fallback lookup if not enough neighbors
        # TODO: there are some duplicated overheads by calling this
        if self.fallback_parent is not None:
            neighbors_fallback = self.fallback_parent.nn_from_payload(
                {**payload, **{'k': k - len(neighbors)}}
            )
            neighbors += neighbors_fallback

        return neighbors[:k]

    def on_post(self, req, resp):
        try:
            payload_json_buf = req.bounded_stream
            payload_json = json.load(payload_json_buf)

            neighbors = self.nn_from_payload(payload_json)

            resp.body = json.dumps(neighbors)
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.body = json.dumps(
                {'Error': f'An internal server error has occurred:\n{e}'})
            resp.status = falcon.HTTP_500

    def set_fallback(self, fallback_parent: 'ANNResource'):
        self.fallback_parent = fallback_parent

    def tojson(self):
        return {
            'path_tar': self.path_tar,
            'ann_meta': self.ann_meta_d,
            'ts_read': self.ts_read_utc.isoformat(),
            'n_ids': len(self.ids),
            'head5_ids': self.ids[:5],
        }


class RefreshResource(object):

    def __init__(self, ann_resource: ANNResource):
        self.ann_resource = ann_resource

    def on_post(self, req, resp):
        """
        Forces a refresh on a specific index
        Note: reloading may take a while if index stored remotely
        Consider running async workers if so (ex. `gunicorn -k gevent`)
        """

        self.ann_resource.load()
        resp.status = falcon.HTTP_200


class MaybeRefreshAllResource(object):

    def __init__(self, ann_resources: List[ANNResource]):
        self.ann_resources = ann_resources

    def on_post(self, req, resp):
        """
        Checks all indexes for freshness -- reloads them if stale
        Note: reloading may take a while if index stored remotely
        Consider running async workers if so (ex. `gunicorn -k gevent`)
        """
        for ann in self.ann_resources:
            ann.maybe_reload()
        resp.status = falcon.HTTP_200


class ANNHealthcheckResource(object):

    def __init__(self, ann_resource: ANNResource):
        self.ann_resource = ann_resource

    def on_get(self, req, resp):
        resp.body = json.dumps(self.ann_resource.tojson())
        resp.status = falcon.HTTP_200


class HealthcheckResource(object):

    def __init__(self, names):
        self.names = names

    def on_get(self, req, resp):
        resp.body = json.dumps(self.names)
        resp.status = falcon.HTTP_200


def build_single_app(path_tar: PathType):
    app = falcon.API()

    ann = ANNResource(path_tar)
    refresh = RefreshResource(ann)
    healthcheck = HealthcheckResource(ann)

    # handle all requests to the '/ann' URL path
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('/query', ann)
    app.add_route('/refresh', refresh)
    app.add_route('/', healthcheck)

    return app


def build_many_app(path_ann_dir: PathType,
                   ooi_table_name: str = None,
                   path_fallback_map: PathType = None,
                   check_reload_interval: int = 3600,
                   ):
    """

    Args:
        path_ann_dir: local or s3 remote path to tarball with
            ANN index, ids, and metadata files
        ooi_table_name: name of dynamo table to grab out-of-index
            vectors from
        path_fallback_map: path to mapping from ANN name
            to name of fallback ANN
        check_reload_interval: if >0, indicates the number of seconds
            between checking for stale indexes and reloading

    Returns: ANN api app

    """
    scheduler = BackgroundScheduler()

    app = falcon.API()
    ann_keys = s3.glob(os.path.join(path_ann_dir, '*.tar*'))
    logging.info(f'{len(ann_keys)} ann indexes detected')

    ooi_table = dynamodb.Table(ooi_table_name) if ooi_table_name else None

    app.req_options.auto_parse_form_urlencoded = True
    ann_d: Dict[str, ANNResource] = {}
    for path_tar in ann_keys:
        ann = ANNResource(path_tar, ooi_table)
        refresh = RefreshResource(ann)
        ann_health = ANNHealthcheckResource(ann)

        ann_name = Path(path_tar).stem.split('.')[0]
        # automatically handles url encoding
        app.add_route(f"/ann/{ann_name}/query", ann)
        app.add_route(f"/ann/{ann_name}/refresh", refresh)
        app.add_route(f"/ann/{ann_name}/", ann_health)

        ann_d[ann_name] = ann
        if check_reload_interval > 0:
            scheduler.add_job(
                func=ann.maybe_reload,
                trigger='interval',
                seconds=check_reload_interval)

    ann_name_l = [falcon.uri.encode(n) for n in ann_d.keys()]

    logging.info('***Done loading all indexes***')

    if path_fallback_map:
        # Linking fallbacks
        logging.info('Linking fallback resources...')
        # TODO: should check that there are no loops in fallback map
        fallback_map = load_fallback_map(path_fallback_map)
        for child, parent in fallback_map.items():
            if child in ann_d:
                ann_d[child].set_fallback(ann_d[parent])
        logging.info('... done linking fallbacks')

    healthcheck = HealthcheckResource(ann_name_l)
    app.add_route('/', healthcheck)

    if check_reload_interval > 0:
        scheduler.start()

    return app
