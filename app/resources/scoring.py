import falcon
from .ann import ANNResource
import json
from typing import List, Dict
import numpy as np


class ScoringResource(object):
    """
    Arbitrary scoring between embeddings

    While ANN resources themselves have a `incl_dist` parameter,
        sometimes we want to just score arbitrary entities without
        the notion of retrieving any neighbors
    """

    def __init__(self, ann_resources: List[ANNResource]):
        # TODO: repeated dict construction as outer scope (app_builder)
        self.ann_resources_d: Dict[str, ANNResource] = {
            a.name: a for a in ann_resources}

    def on_post(self, req, resp):
        neighbors = []
        try:
            # TODO: more efficient to batch-get
            # TODO: limited by ids in same category
            # TODO: maybe have a verbose option and a succinct option

            payload_json_buf = req.bounded_stream
            payload_json = json.load(payload_json_buf)

            ids_1 = json.loads(payload_json.get('ids_1'))
            catalog_1 = payload_json.get('catalog_1')
            ids_2 = json.loads(payload_json.get('ids_2'))
            catalog_2 = payload_json.get('catalog_2')
            dist = payload_json.get('dist') or 'cosine'

            embs_1 = np.array([self.ann_resources_d[catalog_1].get_vector(i)
                               for i in ids_1])
            embs_2 = np.array([self.ann_resources_d[catalog_2].get_vector(i)
                               for i in ids_2])

            norms_1 = np.linalg.norm(embs_1, axis=1)
            norms_2 = np.linalg.norm(embs_2, axis=1)

            dot_arr = embs_1.dot(embs_2.T)
            cos_arr = dot_arr / norms_1 / norms_2[:, None]

            dists_struct = dict(zip(ids_2, cos_arr.tolist()))

            resp.body = json.dumps(dists_struct)
            resp.status = falcon.HTTP_200

        except ValueError:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps([])

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(neighbors)
