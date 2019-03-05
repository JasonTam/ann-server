import falcon
from annoy import AnnoyIndex
import ujson as json
from time import time
from typing import Dict, List, Tuple

S3_URI_PREFIX = 's3://'


def is_s3_path(path: str):
    return path.startswith(S3_URI_PREFIX)


def load_index(path_index: str, n_dim: int) -> AnnoyIndex:
    # The dist metric should be saved in the index
    u = AnnoyIndex(n_dim)

    if is_s3_path(path_index):
        import s3fs
        from tempfile import NamedTemporaryFile
        fs = s3fs.S3FileSystem()
        with fs.open(path_index, 'rb') as f_s3, \
                NamedTemporaryFile(delete=True) as f_tmp:
            f_tmp.write(f_s3.read())
            u.load(f_tmp.name)
    else:
        u.load(path_index)
    return u


def load_ids(path_ids: str) -> Tuple[List[str], Dict[str, int]]:
    if is_s3_path(path_ids):
        import s3fs
        fs = s3fs.S3FileSystem()
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
        # TODO: this needs to be done async so it doesnt block all queries
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
