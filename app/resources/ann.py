import falcon
from annoy import AnnoyIndex
import json
from time import time
from typing import Dict, List, Union, Any, Optional
import boto3
import s3fs
import datetime
from pathlib import Path
from app.io import needs_reload, load_via_tar, load_index, get_dynamo_emb
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

    @property
    def ann_index(self) -> AnnoyIndex:
        return load_index(self.path_index_local, self.ann_meta_d)

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

        ann_index = self.ann_index

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

    def get_vector(self, q_id):
        if q_id in self.ids_d:
            q_ind = self.ids_d[q_id]
            ann_index = self.ann_index
            q_emb = ann_index.get_item_vector(q_ind)
        elif self.ooi_table is not None:
            # Need to look up the vector and query by vector
            q_emb = get_dynamo_emb(
                self.ooi_table, self.ann_meta_d['n_dim'] * DTYPE_FMT, q_id)
        else:
            return None

        return q_emb

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

    def on_get(self, req, resp):
        """Retrieve vector for given id
        If the id exists in index, grab from index.
        If not (item is not active or something), try grabbing from dynamo.
        TODO:
        Finally, if desired, calculate the cold embedding somehow
        """

        q_id = req.params['id']

        q_emb = self.get_vector(q_id)

        if q_emb is None:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(q_emb)

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
