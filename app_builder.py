import falcon
from typing import Dict, Union
import boto3
import s3fs
import os
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from app.resources import *
from app.io import load_fallback_map
import logging

logging.basicConfig(level=logging.INFO)

S3_URI_PREFIX = 's3://'


PathType = Union[Path, str]

s3 = s3fs.S3FileSystem()
dynamodb = boto3.resource('dynamodb')


def build_single_app(path_tar: PathType):
    app = falcon.API()

    ann_r = ANNResource(path_tar)
    refresh_r = RefreshResource(ann_r)
    healthcheck_r = HealthcheckResource(ann_r)

    # handle all requests to the '/ann' URL path
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('/query', ann_r)
    app.add_route('/refresh', refresh_r)
    app.add_route('/', healthcheck_r)

    return app


def build_many_app(path_ann_dir: PathType,
                   ooi_table_name: str = None,
                   path_fallback_map: PathType = None,
                   check_reload_interval: int = 3600,
                   ):
    """

    Args:
        path_ann_dir: local or s3 remote path to tarball with
            ANN index, ids, and metadata files
        ooi_table_name: name of dynamo table to grab out-of-index
            vectors from
        path_fallback_map: path to mapping from ANN name
            to name of fallback ANN
        check_reload_interval: if >0, indicates the number of seconds
            between checking for stale indexes and reloading

    Returns: ANN api app

    """
    scheduler = BackgroundScheduler()

    app = falcon.API()
    ann_keys = s3.glob(os.path.join(path_ann_dir, '*.tar*'))
    logging.info(f'{len(ann_keys)} ann indexes detected')

    ooi_table = dynamodb.Table(ooi_table_name) if ooi_table_name else None

    app.req_options.auto_parse_form_urlencoded = True
    ann_d: Dict[str, ANNResource] = {}
    for path_tar in ann_keys:
        ann_name = Path(path_tar).stem.split('.')[0]

        ann_r = ANNResource(path_tar, ooi_table=ooi_table, name=ann_name)
        refresh_r = RefreshResource(ann_r)
        ann_health_r = ANNHealthcheckResource(ann_r)

        # automatically handles url encoding
        app.add_route(f"/ann/{ann_name}/query", ann_r)
        app.add_route(f"/ann/{ann_name}/refresh", refresh_r)
        app.add_route(f"/ann/{ann_name}/", ann_health_r)

        ann_d[ann_name] = ann_r
        if check_reload_interval > 0:
            scheduler.add_job(
                func=ann_r.maybe_reload,
                trigger='interval',
                seconds=check_reload_interval)

    ann_name_l = [falcon.uri.encode(n) for n in ann_d.keys()]

    logging.info('***Done loading all indexes***')

    if path_fallback_map:
        # Linking fallbacks
        logging.info('Linking fallback resources...')
        # TODO: should check that there are no loops in fallback map
        fallback_map = load_fallback_map(path_fallback_map)
        for child, parent in fallback_map.items():
            if child in ann_d:
                ann_d[child].set_fallback(ann_d[parent])
        logging.info('... done linking fallbacks')

    cross_r = CrossANNResource(list(ann_d.values()))
    app.add_route('/crossq', cross_r)

    healthcheck_r = HealthcheckResource(ann_name_l)
    app.add_route('/', healthcheck_r)

    if check_reload_interval > 0:
        scheduler.start()

    return app
