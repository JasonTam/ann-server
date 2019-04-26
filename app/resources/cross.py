import falcon
from app.resources.ann import ANNResource
import json
from typing import List, Dict


class CrossANNResource(object):
    """
    Operations that cross between multiple (usually 2) ANNResources
    """

    def __init__(self, ann_resources: List[ANNResource]):
        # TODO: repeated dict construction as outer scope (app_builder)
        self.ann_resources_d: Dict[str, ANNResource] = {
            a.name: a for a in ann_resources}

    def on_get(self, req, resp):
        q_name = req.params['q_name']
        q_id = req.params['q_id']
        c_name = req.params['catalog_name']
        k = int(req.params['k'])

        q_emb = self.ann_resources_d[q_name].get_vector(q_id)
        neighbors = self.ann_resources_d[c_name].nn_from_emb(q_emb, k)

        if neighbors is None:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(neighbors)
