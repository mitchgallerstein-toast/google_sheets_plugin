"""
There are two ways to authenticate the Google Analytics Hook.

If you have already obtained an OAUTH token, place it in the password field
of the relevant connection.

If you don't have an OAUTH token, you may authenticate by passing a
'client_secrets' object to the extras section of the relevant connection. This
object will expect the following fields and use them to generate an OAUTH token
on execution.

"type": "service_account",
"project_id": "example-project-id",
"private_key_id": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"private_key": "-----BEGIN PRIVATE KEY-----\nXXXXX\n-----END PRIVATE KEY-----\n",
"client_email": "google-analytics@{PROJECT_ID}.iam.gserviceaccount.com",
"client_id": "XXXXXXXXXXXXXXXXXXXXXX",
"auth_uri": "https://accounts.google.com/o/oauth2/auth",
"token_uri": "https://accounts.google.com/o/oauth2/token",
"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
"client_x509_cert_url": "{CERT_URL}"

More details can be found here:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
"""

import time
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import AccessTokenCredentials

from airflow.hooks.base_hook import BaseHook


class GoogleHook(BaseHook):
    def __init__(self, google_conn_id='google_default'):
        self.google_analytics_conn_id = google_conn_id
        self.connection = self.get_connection(google_conn_id)

        if self.connection.extra_dejson:
            self.client_secrets = self.connection.extra_dejson

    def get_service_object(self,
                           api_name,
                           api_version,
                           scopes=None):
        if self.connection.password:
            credentials = AccessTokenCredentials(self.connection.password,
                                                 'Airflow/1.0')
        elif self.client_secrets:
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.client_secrets,
                                                                           scopes)

        return build(api_name, api_version, credentials=credentials)
