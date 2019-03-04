# locust -f load_testing.py --host=http://127.0.0.1:8000
from pathlib import Path
import ujson as json
from locust import HttpLocust, TaskSet


sample_payload = json.load(
    open(Path(__file__).parent / 'sample_payload.json', 'rb')
)


def get_predict(l):
    l.client.get('/ann')


def post_predict(l):
    headers = {'content-type': 'application/json'}
    l.client.post('/ann', data=json.dumps(sample_payload), headers=headers)


class UserBehavior(TaskSet):
    tasks = {post_predict: 1}


class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 5000
    max_wait = 9000
