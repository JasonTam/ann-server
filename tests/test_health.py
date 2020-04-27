import requests
import json

ENDPOINT = 'http://localhost:8000'


def test_healthcheck():
    r = requests.get(ENDPOINT)

    assert r.status_code == 200
    assert set(json.loads(r.content)) == {'test_ann1', 'test_ann2'}


def test_ann_healthcheck():
    r = requests.get(ENDPOINT + '/ann/test_ann1')

    assert r.status_code == 200
    assert json.loads(r.content)['head5_ids'] == ['0', '1', '2', '3', '4']


def test_query():

    payload = {'id': '0', 'k': 10}

    r = requests.post(ENDPOINT + '/ann/test_ann1/query', json=payload)


def test_query_distance():

    payload = {'id': '0', 'k': 10, 'incl_dist': True}

    r = requests.post(ENDPOINT + '/ann/test_ann1/query', json=payload)


def test_query_score():

    payload = {'id': '0', 'k': 10, 'incl_score': True}

    r = requests.post(ENDPOINT + '/ann/test_ann1/query', json=payload)


def test_query_thresh_score():

    payload = {'id': '0', 'k': 10, 'thresh_score': 0.4}

    r = requests.post(ENDPOINT + '/ann/test_ann1/query', json=payload)


def test_query_distance_thresh_score():

    payload = {'id': '0', 'k': 10, 'thresh_score': 0.4, 'incl_dist': True}

    r = requests.post(ENDPOINT + '/ann/test_ann1/query', json=payload)



def test_cross_query():

    r = requests.get(
        ENDPOINT
        + '/crossq?q_id=0'
        + '&q_name=test_ann1'
        + '&catalog_name=test_ann2'
        + '&k=6'
        # + '&incl_score=True'
        + '&incl_dist=True'
        + '& score & thresh_score = 0.5'
    )
