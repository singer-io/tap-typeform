import requests
import backoff
import singer

from requests.exceptions import ChunkedEncodingError, Timeout, ConnectionError

LOGGER = singer.get_logger()

REQUEST_TIMEOUT = 300

class TypeformError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

class TypeformBadRequestError(TypeformError):
    pass

class TypeformUnauthorizedError(TypeformError):
    pass

class TypeformForbiddenError(TypeformError):
    pass

class TypeformNotFoundError(TypeformError):
    pass

class TypeformTooManyError(TypeformError):
    pass

class TypeformInternalError(TypeformError):
    pass

class TypeformNotAvailableError(TypeformError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": TypeformBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": TypeformUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": TypeformForbiddenError,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": TypeformNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    429: {
        "raise_exception": TypeformTooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded"
    },
    500: {
        "raise_exception": TypeformInternalError,
        "message": "An unhandled error with the Typeform API. Contact the Typeform API team if problems persist."
    },
    503: {
        "raise_exception": TypeformNotAvailableError,
        "message": "API service is currently unavailable."
    }
}

class Client(object):
    BASE_URL = 'https://api.typeform.com'

    def __init__(self, config):
        self.token = 'Bearer ' + config.get('token')
        self.metric = config.get('metric')
        self.session = requests.Session()
        # Set and pass request timeout to config param `request_timeout` value.
        config_request_timeout = config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then it set default to 300 seconds.

    def build_url(self, endpoint):
        return f"{self.BASE_URL}/{endpoint}"

    @backoff.on_exception(backoff.expo,
                          (Timeout, ConnectionError), # Backoff for Timeout and ConnectionError.
                          max_tries=5,
                          factor=2)
    @backoff.on_exception(backoff.expo,
                          (TypeformInternalError, TypeformNotAvailableError,
                           TypeformTooManyError, ChunkedEncodingError),
                          max_tries=3,
                          factor=2)
    def request(self, method, url, params=None, **kwargs):
        # note that typeform response api doesn't return limit headers

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization'] = self.token

        request = requests.Request(method, url, headers=kwargs['headers'], params=params)

        response = self.session.send(request.prepare(), timeout=self.request_timeout)# Pass request timeout

        if response.status_code != 200:
            raise_for_error(response)
            return None

        if 'total_items' in response.json():
            LOGGER.info('raw data items= {}'.format(response.json()['total_items']))
        return response.json()

    # Max page size for forms API is 200
    def get_forms(self, page_size=200):
        url = self.build_url(endpoint='forms')
        with singer.metrics.http_request_timer(endpoint=url):
            return self._get_forms('get', url, page_size)

    def _get_forms(self, method, url, page_size):
        page = 1
        paginate = True
        records = []
        params = {'page_size': page_size}

        while paginate:
            params['page'] = page
            response = self.request(method, url, params=params)
            page_count = response.get('page_count')
            paginate = page_count > page
            page += 1

            records += response.get('items')

        return records

    def get_form_definition(self, form_id, **kwargs):
        endpoint = f"forms/{form_id}"
        url = self.build_url(endpoint=endpoint)
        with singer.metrics.http_request_timer(endpoint=url):
            try:
                return self.request('get', url, **kwargs)
            except TypeformForbiddenError as err:
                raise RuntimeError("Maybe add the Forms:Read scope to your token") from err

    def get_form_responses(self, form_id, **kwargs):
        endpoint = f"forms/{form_id}/responses"
        url = self.build_url(endpoint)
        with singer.metrics.http_request_timer(endpoint=url):
            try:
                return self.request('get', url, **kwargs)
            except TypeformForbiddenError as err:
                raise RuntimeError("Maybe add the Responses:Read scope to your token") from err


def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = response.status_code

            # Handling status code 429 specially since the required information is present in the headers
            if error_code == 429:
                resp_headers = response.headers
                api_rate_limit_message = ERROR_CODE_EXCEPTION_MAPPING[429]["message"]
                message = "HTTP-error-code: 429, Error: {}. Please retry after {} seconds".format(api_rate_limit_message, resp_headers.get("Retry-After"))

            # Handling status code 403 specially since response of API does not contain enough information
            elif error_code in (403, 401):
                api_message = ERROR_CODE_EXCEPTION_MAPPING[error_code]["message"]
                message = "HTTP-error-code: {}, Error: {}".format(error_code, api_message)
            else:
                # Forming a response message for raising custom exception
                try:
                    response_json = response.json()
                except Exception:
                    response_json = {}

                message = "HTTP-error-code: {}, Error: {}".format(
                    error_code,
                    response_json.get("description", "Uknown Error"))

            exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", TypeformError)
            message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "")
            formatted_message = f"HTTP-error-code: {error_code}, Error: {message}"
            raise exc(formatted_message, response) from None

        except (ValueError, TypeError):
            raise TypeformError(error) from None
