import falcon
from annoy import AnnoyIndex
import json
from time import time
from typing import Dict, List, Tuple, Union, Any
import s3fs
import os
import datetime
import tarfile
from pathlib import Path

S3_URI_PREFIX = 's3://'


PATH_TMP = Path('/tmp')
PATH_ANN_REMOTE = 's3://jason-garbage/ann/cats.tar'
ANN_INDEX_KEY = 'index.ann'
ANN_IDS_KEY = 'ids.txt'
ANN_META_KEY = 'metadata.json'
PATH_TIMESTAMP_LOCAL = '/tmp/timestamp.txt'

PathType = Union[Path, str]


def is_s3_path(path: PathType):
    return str(path).startswith(S3_URI_PREFIX)


def needs_reload(path_tar: PathType) -> bool:
    if not os.path.isfile(PATH_TIMESTAMP_LOCAL):
        # Fresh container needs to download index
        return True
    else:
        # Container re-use: check if new index exists on remote
        fs = s3fs.S3FileSystem()  # TODO: move to outer scope?
        local_mtime = datetime.datetime.fromtimestamp(
            int(open(PATH_TIMESTAMP_LOCAL, 'r').read().strip()),
            tz=datetime.timezone.utc)
        remote_mtime = fs.info(path_tar)['LastModified']

        return remote_mtime > local_mtime


def load_via_tar(path_tar: PathType = PATH_ANN_REMOTE):
    fs = s3fs.S3FileSystem()
    # Write to timestamp file
    # Note: `fromisoformat` only in Py3.7
    # ts_read = datetime.datetime.utcnow().isoformat()
    ts_read = int(time())
    with open(PATH_TIMESTAMP_LOCAL, 'w') as f:
        f.write(str(ts_read))
    ann_tar = tarfile.open(fileobj=fs.open(path_tar, 'rb'))
    ann_tar.extractall(PATH_TMP)

    ann_index, meta_d = load_index(
        PATH_TMP/ANN_INDEX_KEY,
        PATH_TMP/ANN_META_KEY,
    )
    ann_ids, ann_ids_d = load_ids(PATH_TMP/ANN_IDS_KEY)

    return ann_index, ann_ids, ann_ids_d, ts_read, meta_d


def load_index(path_index: PathType, path_meta: PathType) -> AnnoyIndex:
    meta_d = json.load(open(path_meta, 'r'))
    n_dim = meta_d['n_dim']
    metric = meta_d['metric']
    u = AnnoyIndex(
        n_dim,
        metric=metric,
    )
    u.load(str(path_index))
    return u, meta_d


def load_ids(path_ids: PathType) -> Tuple[List[str], Dict[str, int]]:
    if is_s3_path(path_ids):
        fs = s3fs.S3FileSystem()  # TODO: move to outer scope?
        open_fn = fs.open
    else:
        open_fn = open

    ids = [s.decode('utf-8') for s in
           open_fn(path_ids, 'rb').read().splitlines()]
    ids_d = dict(zip(ids, range(len(ids))))
    return ids, ids_d


class ANNResource(object):

    def __init__(self, path_tar: PathType):
        self.path_tar = path_tar

        self.index: AnnoyIndex = None
        self.ids: List[Any] = None
        self.ids_d: Dict[Any, int] = None
        self.ts_read: int = None
        self.ann_meta_d: Dict[str, Any] = None

        self.load()

    def load(self, path_tar: str = None):
        path_tar = path_tar or self.path_tar
        tic = time()
        print(f'Loading: {path_tar}')
        self.index, self.ids, self.ids_d, self.ts_read, self.ann_meta_d = \
            load_via_tar(path_tar)
        print(f'...Done Loading! [{time() - tic} s]')

    def on_post(self, req, resp):

        # Check if our index is up to date (needs to be fast 99% of time)
        # and reload if out of date
        # Yes, we do this every time TODO: only do this on keep-warm calls
        if needs_reload(self.path_tar):
            self.load()

        try:
            payload_json_buf = req.bounded_stream
            payload_json = json.load(payload_json_buf)
            k = payload_json['k']
            neighbors_l = []
            for q_id in payload_json['ids']:
                q_ind = self.ids_d[q_id]
                neighbors = [self.ids[ind] for ind in
                             self.index.get_nns_by_item(q_ind, k)]
                neighbors_l.append(neighbors)

            resp.body = json.dumps(neighbors_l)
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.body = json.dumps(
                {'Error': f'An internal server error has occurred:\n{e}'})
            resp.status = falcon.HTTP_500

    def tojson(self):
        return {
            'path_tar': self.path_tar,
            'ann_meta': self.ann_meta_d,
            'ts_read': self.ts_read,
            'n_ids': len(self.ids),
            'head5_ids': self.ids[:5],
        }


class RefreshResource(object):

    def __init__(self, ann_resource: ANNResource):
        self.ann_resource = ann_resource

    def on_post(self, req, resp):
        """
        Note: reloading may take a while if index stored remotely
        Consider running async workers if so (ex. `gunicorn -k gevent`)
        """

        self.ann_resource.load()
        resp.status = falcon.HTTP_200


class HealthcheckResource(object):

    def __init__(self, ann_resource: ANNResource):
        self.ann_resource = ann_resource

    def on_get(self, req, resp):
        resp.body = json.dumps(self.ann_resource.tojson())
        resp.status = falcon.HTTP_200


def build_app(path_tar: PathType = PATH_ANN_REMOTE):
    app = falcon.API()

    ann = ANNResource(path_tar)
    refresh = RefreshResource(ann)
    healthcheck = HealthcheckResource(ann)

    # handle all requests to the '/ann' URL path
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('/ann', ann)
    app.add_route('/refresh', refresh)
    app.add_route('/', healthcheck)

    return app
