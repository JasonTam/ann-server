import falcon
from .ann import ANNResource
import json
import os
from time import sleep


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


class TmpSpaceResource(object):

    def on_get(self, req, resp):
        """
        Returns: Size of /tmp in mb

        """
        def get_size(start_path='/tmp'):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(start_path):
                for f in filenames:
                    try:
                        fp = os.path.join(dirpath, f)
                        total_size += os.path.getsize(fp)
                    except FileNotFoundError:
                        pass
                    except:
                        pass
            return total_size

        size_mb = int(get_size('/tmp') / 1e6)

        resp.body = json.dumps(size_mb)
        resp.status = falcon.HTTP_200


class SleepResource(object):

    def on_get(self, req, resp):
        """param `duration` in milliseconds """

        duration = req.params.get('duration') or 100
        sleep(float(duration) / 1000.)
        resp.status = falcon.HTTP_200
