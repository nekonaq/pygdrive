# stolen from https://github.com/nithinmurali/pygsheets
# pygsheets/authorization.py
#
import os
import json
from google.oauth2 import service_account
from pygsheets.authorization import _SCOPES, _get_user_authentication_credentials

from .client import Client


def authorize(
        client_secret='client_secret.json',
        service_account_file=None,
        service_account_env_var=None,
        credentials_directory='',
        scopes=_SCOPES,
        custom_credentials=None,
        local=False,
        http=None,
        check=True,
        client_class=Client,
        client_initkwargs=None,
):
    if custom_credentials is not None:
        credentials = custom_credentials
    elif service_account_env_var is not None:
        service_account_info = json.loads(os.environ[service_account_env_var])
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=scopes)
    elif service_account_file is not None:
        credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    else:
        credentials = _get_user_authentication_credentials(client_secret, scopes, credentials_directory, local)

    return client_class(credentials, http=http, check=check, **client_initkwargs or {})
