import falcon
from .ann import ANNResource
import json


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
