import falcon
from typing import Dict, Union
import boto3
import s3fs
import os
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import logging
try:
    from .app.resources import *
    from .app.io import load_fallback_map
except ImportError:
    from app.resources import *
    from app.io import load_fallback_map

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
    ann_keys = s3.glob(os.path.join(path_ann_dir, '**.tar*'))
    logging.info(f'{len(ann_keys)} ann indexes detected')

    ooi_dynamo_table = None
    ooi_ann_name = None
    if ooi_table_name:
        dynamo_tables = {t.name for t in dynamodb.tables.all()}
        if ooi_table_name in dynamo_tables:
            logging.info(f'Using {ooi_table_name} dynamo table for OOI lookup')
            ooi_dynamo_table = dynamodb.Table(ooi_table_name)
        else:
            logging.info(f'Using {ooi_table_name} ANN (if exists) '
                         f'for OOI lookup ')
            ooi_ann_name = ooi_table_name

    app.req_options.auto_parse_form_urlencoded = True
    ann_d: Dict[str, ANNResource] = {}
    for path_tar in ann_keys:
        # ann_name = Path(path_tar).stem.split('.')[0]
        # Preserve prefix structure of glob
        ann_name = path_tar.split(path_ann_dir.split(S3_URI_PREFIX)[-1])[-1]

        ann_r = ANNResource(path_tar,
                            ooi_dynamo_table=ooi_dynamo_table,
                            name=ann_name)
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

    if ooi_ann_name:
        # Linking OOI ann
        # (if the query is OOI for an ann, look at this other ann for the emb)
        logging.info('Linking ooi_ann to resources...')
        for name, ann_r in ann_d.items():
            if name != ooi_ann_name:
                ann_r.ooi_ann = ann_d[ooi_ann_name]
        logging.info('... done linking ooi_ann_name')

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

    maybe_refresh_all_r = MaybeRefreshAllResource(list(ann_d.values()))
    app.add_route('/refresh-all', maybe_refresh_all_r)

    tmpspace_r = TmpSpaceResource()
    app.add_route('/tmp', tmpspace_r)

    sleep_r = SleepResource()
    app.add_route('/sleep', sleep_r)

    if check_reload_interval > 0:
        scheduler.start()

    return app
