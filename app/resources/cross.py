import falcon
from .ann import ANNResource
import json
from typing import List, Dict


class CrossANNResource(object):
    """
    Operations that cross between multiple (usually 2) ANNResources
    """

    def __init__(self, ann_resources: List[ANNResource],
                 fallback_dynamo_table=None):
        # TODO: repeated dict construction as outer scope (app_builder)
        self.ann_resources_d: Dict[str, ANNResource] = {
            a.name: a for a in ann_resources}

        self.fallback_dynamo_table = fallback_dynamo_table

    def on_get(self, req, resp):
        q_name = req.params['q_name']
        q_id = req.params['q_id']
        c_name = req.params['catalog_name']
        k = int(req.params['k'])
        incl_dist = bool(req.params.get('incl_dist')) or False

        neighbors = []
        try:
            if q_name in self.ann_resources_d:
                q_emb = self.ann_resources_d[q_name].get_vector(q_id)
            elif self.fallback_dynamo_table is not None:
                # Need to look up the vector and query by vector
                q_emb = self.fallback_dynamo_table(
                    self.fallback_dynamo_table, q_id)
            else:
                raise ValueError(f'ANN: {q_name} not found '
                                 f'and dynamo fallback failed')

            neighbors = self.ann_resources_d[c_name].nn_from_emb(
                    q_emb, k, incl_dist=incl_dist)
        except ValueError:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps([])

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(neighbors)
