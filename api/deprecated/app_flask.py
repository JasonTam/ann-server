from flask import Flask, request, jsonify
from annoy import AnnoyIndex
import ujson as json
from time import time


app = Flask(__name__)


def load_index(path_index, n_dim) -> AnnoyIndex:
    # The dist metric should be saved in the index
    u = AnnoyIndex(n_dim)
    u.load(path_index)  # super fast, will just mmap the file
    return u


tic = time()
path_index = '/home/jason/Documents/benchmarks/saved/glove1m.ann'
path_ids = '/home/jason/Documents/benchmarks/saved/glove1m_ids'
n_dim = 25
index = load_index(path_index, n_dim)
ids = open(path_ids, 'r').read().splitlines()
ids_d = dict(zip(ids, range(len(ids))))
print(f'Done Initializing! [{time() - tic} s]')


@app.route("/ann", methods=["GET", "POST"])
def ann():
    payload_json = request.get_json()
    k = payload_json['k']
    neighbors_l = []
    for q_id in payload_json['ids']:
        q_ind = ids_d[q_id]
        neighbors = [ids[ind] for ind in
                     index.get_nns_by_item(q_ind, k)]
        neighbors_l.append(neighbors)

    return jsonify(neighbors_l)


if __name__ == '__main__':
    app.run()
