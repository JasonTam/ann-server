import falcon
from app.resources.ann import ANNResource
from typing import List


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
