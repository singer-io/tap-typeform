import requests
import backoff
import singer

from datetime import timedelta
from singer.utils import now
from requests.exceptions import ChunkedEncodingError, Timeout, ConnectionError
from tap_typeform.utils import write_config


LOGGER = singer.get_logger()

REQUEST_TIMEOUT = 300
MAX_RESPONSES_PAGE_SIZE = 1000
FORMS_PAGE_SIZE = 200

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

            # Handling status code 403 specially since the response of API does not contain enough information
            elif error_code in (403, 401):
                api_message = ERROR_CODE_EXCEPTION_MAPPING[error_code]["message"]
                message = "HTTP-error-code: {}, Error: {}".format(error_code, api_message)
            else:
                # Forming a response message for raising a custom exception
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
    """
    The client class is used for making REST calls to the Github API.
    """
    BASE_URL = 'https://api.typeform.com'
    OAUTH_URL = 'https://api.typeform.com/oauth/token'

    def __init__(self, config, config_path, dev_mode):
        self.metric = config.get('metric')
        self.session = requests.Session()
        self.page_size = MAX_RESPONSES_PAGE_SIZE
        self.form_page_size = FORMS_PAGE_SIZE
        self.config_path = config_path
        self.get_page_size(config)

        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.refresh_token = config.get('refresh_token')
        self.access_token = config.get('token')
        self.dev_mode = dev_mode
        self.refresh()

        # Set and pass request timeout to config param `request_timeout` value.
        config_request_timeout = config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then it set default to 300 seconds.

    @backoff.on_exception(backoff.expo,(Timeout, ConnectionError),  # Backoff for Timeout and ConnectionError.
                            max_tries=5, factor=2, jitter=None)
    @backoff.on_exception(backoff.expo, (TypeformInternalError, TypeformNotAvailableError, TypeformTooManyError, ChunkedEncodingError),
                            max_tries=3, factor=2)
    def refresh(self):
        """
        Refreshes access token and refresh token
        """
        # Existing connections won't have refresh token so use the existing access token
        if not self.refresh_token:
            return

        # In dev mode, don't refresh access token
        if self.dev_mode:
            if not self.access_token:
                raise Exception('Access token is missing')

            return

        response = self.session.post(url=self.OAUTH_URL,
                                     headers={
                                         'Content-Type': 'application/x-www-form-urlencoded'},
                                     data={'client_id': self.client_id,
                                           'client_secret': self.client_secret,
                                           'refresh_token': self.refresh_token,
                                           'grant_type': 'refresh_token',
                                           'scope': 'forms:read accounts:read images:read responses:read themes:read workspaces:read offline'})

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.refresh_token = data['refresh_token']
        self.access_token = data['access_token']

        write_config(self.config_path,
                     {'refresh_token': self.refresh_token,
                      'token': self.access_token})

    def get_page_size(self, config):
        """
        This function will get page size from config,
        and will return the default value if invalid page size is given.
        """
        page_size = config.get('page_size')
        if page_size is None:
            return
        if ((type(page_size) == int or type(page_size) == float) and (page_size > 0)) or \
                (type(page_size) == str and page_size.replace('.', '', 1).isdigit() and (float(page_size) > 0)):
            self.page_size = int(float(page_size))
            self.form_page_size = min(self.form_page_size, self.page_size)
        else:
            raise Exception(f"The entered page size is invalid, it should be a valid integer.")

    def build_url(self, endpoint):
        """
        Returns full URL for a given endpoint.
        """
        return f"{self.BASE_URL}/{endpoint}"

    @backoff.on_exception(backoff.expo, (Timeout, ConnectionError),  # Backoff for Timeout and ConnectionError.
                          max_tries=5, factor=2, jitter=None)
    @backoff.on_exception(backoff.expo, (TypeformInternalError, TypeformNotAvailableError, TypeformTooManyError, ChunkedEncodingError),
                          max_tries=3, factor=2)
    def request(self, url, params={}, **kwargs):
        """
        Call rest API and return the response in case of status code 200.
        """

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.access_token:
            kwargs['headers']['Authorization'] = 'Bearer ' + self.access_token

        LOGGER.info("URL: %s and Params: %s", url, params)
        response = self.session.get(url, params=params, headers=kwargs['headers'], timeout=self.request_timeout)
        if response.status_code != 200:
            raise_for_error(response)

        if 'total_items' in response.json():
            LOGGER.info('raw data items= {}'.format(response.json()['total_items']))
        return response.json()
