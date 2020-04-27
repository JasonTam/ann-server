import falcon
from .ann import ANNResource, dist_to_score
from ..io import needs_reload, load_via_tar, load_index, get_dynamo_emb
import json
from typing import List, Dict
from distutils.util import strtobool


class CrossANNResource(object):
    """
    ANN lookups that cross between multiple (usually 2) ANNResources
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
        incl_dist = strtobool(req.params.get('incl_dist')) or False
        incl_score = strtobool(req.params.get('incl_score')) or False
        thresh_score = req.params.get('thresh_score')
        thresh_score = float(thresh_score) if thresh_score else False
        include_distances = bool(incl_dist or incl_score or thresh_score)

        neighbors = []
        try:
            if q_name in self.ann_resources_d:
                q_emb = self.ann_resources_d[q_name].get_vector(q_id)
            elif self.fallback_dynamo_table is not None:
                # Need to look up the vector and query by vector
                q_emb = get_dynamo_emb(self.fallback_dynamo_table, q_id)
            else:
                raise ValueError(f'ANN: {q_name} not found '
                                 f'and dynamo fallback failed')

            neighbors = self.ann_resources_d[c_name].nn_from_emb(
                q_emb, k,
                incl_dist=include_distances
            )

            if thresh_score:
                neighbors = [n for n in neighbors if n.score > thresh_score]

            recs = [n.to_dict(incl_dist, incl_score) for n in neighbors]

            res = {
                'recs': recs,
                'id_type': '-',
            }

            resp.body = json.dumps(res)
            resp.status = falcon.HTTP_200

        except ValueError:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps([])


