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

def raise_for_error(response):
    """
    Retrieve the error code and the error message from the response and return custom exceptions accordingly.
    """
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
                    response_json.get("description", "Unknown Error"))

            exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", TypeformError)
            message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "")
            formatted_message = f"HTTP-error-code: {error_code}, Error: {message}"
            raise exc(formatted_message, response) from None

        except (ValueError, TypeError):
            raise TypeformError(error) from None

class Client(object):
    BASE_URL = 'https://api.typeform.com'

    def __init__(self, config):
        self.token = 'Bearer ' + config.get('token')
        self.metric = config.get('metric')
        self.session = requests.Session()
        self.fetch_uncompleted_forms = config.get('fetch_uncompleted_forms', False)

        # Set and pass request timeout to config param `request_timeout` value.
        config_request_timeout = config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then it set default to 300 seconds.

    def build_url(self, endpoint):
        return f"{self.BASE_URL}/{endpoint}"

    @backoff.on_exception(backoff.expo,(Timeout, ConnectionError), # Backoff for Timeout and ConnectionError.
                            max_tries=5, factor=2)
    @backoff.on_exception(backoff.expo, (TypeformInternalError, TypeformNotAvailableError, TypeformTooManyError, ChunkedEncodingError),
                            max_tries=3, factor=2)
    def request(self, url, params=None, **kwargs):

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization'] = self.token

        response = self.session.get(url, params=params, headers=kwargs['headers'], timeout=self.request_timeout)

        if response.status_code != 200:
            raise_for_error(response)

        if 'total_items' in response.json():
            LOGGER.info('raw data items= {}'.format(response.json()['total_items']))
        return response.json()
