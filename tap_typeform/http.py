import requests
import backoff
import singer

LOGGER = singer.get_logger()

class RateLimitException(Exception):
    pass

class MetricsRateLimitException(Exception):
    pass

class Client(object):
    BASE_URL = 'https://api.typeform.com'

    def __init__(self, config):
        self.token = 'Bearer ' + config.get('token')
        self.metric = config.get('metric')
        self.session = requests.Session()

    def build_url(self, endpoint):
        return f"{self.BASE_URL}/{endpoint}"

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, method, url, **kwargs):
        # note that typeform response api doesn't return limit headers

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization'] = self.token

        response = requests.request(method, url, **kwargs)

        if response.status_code in [429, 503]:
            raise RateLimitException()
        if response.status_code == 423:
            raise MetricsRateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.error('{} - {}'.format(response.status_code, response.text))
            raise
        if 'total_items' in response.json():
            LOGGER.info('raw data items= {}'.format(response.json()['total_items']))
        return response.json()

    def get_forms(self, **kwargs):
        url = self.build_url(endpoint='forms')
        return self.request('get', url, **kwargs)

    def get_form_definition(self, form_id, **kwargs):
        endpoint = f"forms/{form_id}"
        url = self.build_url(endpoint=endpoint)
        return self.request('get', url, **kwargs)

    def get_form_responses(self, form_id, **kwargs):
        endpoint = f"forms/{form_id}/responses"
        url = self.build_url(endpoint)
        return self.request('get', url, **kwargs)
