# TODO: this is awful. the generated python thrift service is shit

import falcon
import ujson as json
from time import time

import sys, glob

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from protocol import QueryService
from protocol import ttypes

from datetime import datetime


host = 'localhost'
port = 10000
k = 5
search_range = None
queryTimeParams = ''
retObj = False
retExternId = False


class ANNResource(object):

    def __init__(self, path_index, path_ids, n_dim):
        # Make socket
        self.socket = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.socket)
        # Wrap in a protocol
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        self.client = QueryService.Client(self.protocol)


    def on_post(self, req, resp):
        queries = [
            '-2.857199907302856445e-01 1.603000044822692871e+00 -2.336899936199188232e-01 4.247600138187408447e-01 7.183499634265899658e-02 -1.663300037384033203e+00 -6.774700284004211426e-01 -2.006600052118301392e-01 7.255899906158447266e-01 -7.225999832153320312e-01 9.668300300836563110e-02 1.044299960136413574e+00 1.196400046348571777e+00 -2.735399901866912842e-01 1.441599965095520020e+00 6.502100080251693726e-02 9.345399737358093262e-01 -4.057500064373016357e-01 9.226999878883361816e-01 -2.960099875926971436e-01 -5.180299878120422363e-01 8.512099981307983398e-01 -1.033900022506713867e+00 5.065599828958511353e-02 1.396400034427642822e-01',
            '-2.312099933624267578e+00 -1.069100022315979004e+00 3.303000032901763916e-01 -8.492599725723266602e-01 -4.509800076484680176e-01 -1.110299944877624512e+00 -2.791800022125244141e+00 -3.408899903297424316e-01 1.488899946212768555e+00 6.055299937725067139e-02 -1.195600032806396484e+00 -3.486599922180175781e-01 2.411700040102005005e-01 1.770300030708312988e+00 -1.250100016593933105e+00 -5.211899876594543457e-01 7.655300199985504150e-02 -1.166900038719177246e+00 3.468300104141235352e-01 1.557800054550170898e+00 8.309700340032577515e-02 -2.923000097274780273e+00 1.978000044822692871e+00 -3.480600118637084961e-01 8.244900107383728027e-01',
        ]

        try:
            self.transport.open()

            queryObj = ''
            for s in queries:
                queryObj = queryObj + s + '\n'

            if queryTimeParams != '':
                self.client.setQueryTimeParams(queryTimeParams)

            res = None
            if not k is None:
                if not search_range is None:
                    raise ValueError('Range search is not allowed if the KNN search is specified!')

                res = self.client.knnQuery(k, queryObj, retObj, retExternId)
            elif not search_range is None:
                r = search_range
                if not k is None:
                    raise ValueError('KNN search is not allowed if the range search is specified')

                print("Running range search, range=%f" % r)
                res = self.client.rangeQuery(r, queryObj, retObj, retExternId)
            else:
                raise ValueError("Wrong search type")

            for e in res:
                s = ''
                if retExternId:
                    s = 'externId=' + e.externId
                print("id=%d dist=%f %s" % (e.id, e.dist, s))
                if retObj:
                    print(e.obj)

            self.transport.close()

            neighbors_l = [[1]]
            resp.body = json.dumps(neighbors_l)
            resp.status = falcon.HTTP_200

        except Exception as e:
            resp.body = json.dumps(
                {'Error': f'An internal server error has occurred:\n{e}'})
            resp.status = falcon.HTTP_500


class HealthcheckResource(object):

    def __init__(self, ann_resource):
        self.ann_resource = ann_resource

    def on_get(self, req, resp):
        resp.body = str(self.ann_resource.index.f)
        resp.status = falcon.HTTP_200


def build_app(
        path_index='/home/jason/Documents/benchmarks/saved/glove1m.ann',
        path_ids='/home/jason/Documents/benchmarks/saved/glove1m_ids',
        n_dim=25,
):
    app = falcon.API()

    ann = ANNResource(path_index, path_ids, n_dim)
    healthcheck = HealthcheckResource(ann)

    # handle all requests to the '/ann' URL path
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('/ann', ann)
    app.add_route('/', healthcheck)

    return app
