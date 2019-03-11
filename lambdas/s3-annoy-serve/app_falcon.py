import falcon
from annoy import AnnoyIndex
import ujson as json
from time import time
from typing import Dict, List, Tuple
import s3fs
import os
import datetime
import tarfile
from pathlib import Path

S3_URI_PREFIX = 's3://'


PATH_TMP = Path('/tmp')
PATH_ANN_REMOTE = 's3://jason-garbage/ann/glove100/crap.tar'
ANN_INDEX_KEY = 'index.ann'
ANN_IDS_KEY = 'ids.txt'
ANN_META_KEY = 'metadata.json'
PATH_TIMESTAMP_LOCAL = '/tmp/timestamp.txt'


# TODO: part of remote load fn
fs = s3fs.S3FileSystem()
# Write to timestamp file
ann_tar = tarfile.open(fileobj=fs.open(PATH_ANN_REMOTE, 'rb'))
ann_tar.extractall(PATH_TMP)
#


def is_s3_path(path: str):
    return path.startswith(S3_URI_PREFIX)


def needs_reload(path_ann_tar: str) -> bool:
    if not os.path.isfile(PATH_TIMESTAMP_LOCAL):
        # Fresh container needs to download index
        return True
    else:
        # Container re-use: check if new index exists on remote
        fs = s3fs.S3FileSystem()  # TODO: move to outer scope?
        local_mtime = datetime.datetime.fromtimestamp(
            # os.path.getmtime(PATH_ANN_LOCAL),
            int(open(PATH_TIMESTAMP_LOCAL, 'r').read().strip()),
            tz=datetime.timezone.utc)
        remote_mtime = fs.info(path_ann_tar)['LastModified']

        return remote_mtime > local_mtime


def load_index(path_index: str, path_meta: str) -> AnnoyIndex:
    # The dist metric should be saved in the index
    n_dim = None  # TODO: should read from path_meta
    u = AnnoyIndex(n_dim)

    if is_s3_path(path_index):
        fs = s3fs.S3FileSystem()  # TODO: move to outer scope?
        fs.get(path_index, PATH_TMP / ANN_INDEX_KEY)
        u.load(PATH_TMP / ANN_INDEX_KEY)
    else:
        u.load(path_index)
    return u


def load_ids(path_ids: str) -> Tuple[List[str], Dict[str, int]]:
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

    def __init__(self, path_index: str, path_ids: str, n_dim: int):
        self.path_index = path_index
        self.path_ids = path_ids
        self.n_dim = n_dim

        self.index = None
        self.ids, self.ids_d = None, None

        self.load()

    def load(self, path_index=None, path_ids=None, n_dim=None):
        tic = time()
        self.index = load_index(
            path_index or self.path_index, n_dim or self.n_dim)
        self.ids, self.ids_d = load_ids(path_ids or self.path_ids)
        print(f'Done Loading! [{time() - tic} s]')

    def on_post(self, req, resp):

        # Check if our index is up to date (needs to be fast 99% of time)
        # and reload if out of date
        # Yes, we do this every time TODO: only do this on keep-warm calls
        if needs_reload(self.path_index):
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
            'path_index': self.path_index,
            'path_ids': self.path_ids,
            'n_dim': self.n_dim,
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


def build_app(path_index: str, path_ids: str, n_dim: int):
    app = falcon.API()

    ann = ANNResource(path_index, path_ids, n_dim)
    refresh = RefreshResource(ann)
    healthcheck = HealthcheckResource(ann)

    # handle all requests to the '/ann' URL path
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('/ann', ann)
    app.add_route('/refresh', refresh)
    app.add_route('/', healthcheck)

    return app
