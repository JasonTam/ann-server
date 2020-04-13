import tarfile
import random
from annoy import AnnoyIndex
from pathlib import Path
from datetime import datetime
from io import BytesIO
import json
from shutil import copyfileobj


CUR_DIR = Path(__file__).parent
METRIC = 'angular'
FACTORS_KEY = 'factors'
ID_KEY = 'id'
N_TREES = 10
PATH_DISK_SAVE = '/tmp/index.ann'
IDS_NAME = 'ids.txt'
INDEX_NAME = 'index.ann'
META_NAME = 'metadata.json'
N_DIM = 40


def create_tarball(ids, meta_d):
    # Create tarball buffer
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar_buf:
        # Add index
        tar_buf.add(PATH_DISK_SAVE, arcname=INDEX_NAME)

        # Add ids
        info = tarfile.TarInfo(name=IDS_NAME)
        ids_bytes = '\n'.join(map(str, ids)).encode('utf-8')
        ids_buf = BytesIO(ids_bytes)
        info.size = len(ids_bytes)
        tar_buf.addfile(tarinfo=info, fileobj=ids_buf)

        # Add Metadata
        info = tarfile.TarInfo(name=META_NAME)
        meta_bytes = json.dumps(meta_d).encode('utf-8')
        meta_buf = BytesIO(meta_bytes)
        info.size = len(meta_bytes)
        tar_buf.addfile(tarinfo=info, fileobj=meta_buf)
    return buf


def make_ann(n_dim=N_DIM, n_items=100):
    ids = []
    ann = AnnoyIndex(n_dim, METRIC)
    ann.on_disk_build(PATH_DISK_SAVE)

    for ind in range(n_items):
        v = [random.gauss(0, 1) for _ in range(n_dim)]
        ann.add_item(ind, v)
        ids.append(str(ind))

    ann.build(N_TREES)

    meta_d = {
        'vec_src': Path(__file__).name,
        'metric': METRIC,
        'n_dim': n_dim,
        'timestamp_utc': datetime.utcnow().isoformat(),
    }

    return ids, meta_d


def make_ann_tar(name):

    ids, meta_d = make_ann()
    buf = create_tarball(ids, meta_d)
    buf.seek(0)
    with open(CUR_DIR / 'fixtures' / f'{name}.tar.gz', 'wb') as fo:
        copyfileobj(buf, fo)


if __name__ == '__main__':
    make_ann_tar('test_ann1')
    make_ann_tar('test_ann2')

