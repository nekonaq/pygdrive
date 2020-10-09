import os
import logging
from google_auth_httplib2 import AuthorizedHttp
import pygsheets.client
import pygsheets.sheet

from .functional import cached_property
from .drive import DriveAPIWrapper
from .files import FilesAPIWrapper


class Client(pygsheets.client.Client):
    sheet_api_class = pygsheets.sheet.SheetAPIWrapper
    drive_api_class = DriveAPIWrapper
    files_api_class = FilesAPIWrapper

    sheet_initkwargs_default = {
        'retries': 3,
        'check': True,
        'seconds_per_quota': 100,
    }

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    # このファイル 'client.py' と同じ場所にディレクトリ 'data' 以下を置かないことで、
    # SheetAPIWrapper DriveAPIWrapper が行うサービス・ディスカバリを
    # discover.build_from_document() ではなく discover.build() で行う。

    def __init__(
            self, credentials,
            retries=None,
            http=None,
            check=None,
            seconds_per_quota=None,
            sheet_initkwargs=None,
            drive_initkwargs=None,
            files_initkwargs=None,
    ):
        self.oauth = credentials
        self.logger = logging.getLogger(__name__)
        self.http = AuthorizedHttp(credentials, http=http)

        self.sheet_initkwargs = sheet_initkwargs or {}
        for key, argval in (('retries', retries), ('check', check), ('seconds_per_quota', seconds_per_quota)):
            if argval is not None:
                self.sheet_initkwargs[key] = argval

        self.drive_initkwargs = drive_initkwargs or {}
        self.files_initkwargs = files_initkwargs or {}

    def get_sheet_api(self, **kwargs):
        initkwargs = self.sheet_initkwargs.copy()
        initkwargs.update(kwargs)
        return self.sheet_api_class(self.http, self.data_path, **initkwargs)

    @cached_property
    def sheet(self):
        return self.get_sheet_api()

    def get_drive_api(self, **kwargs):
        initkwargs = self.drive_initkwargs.copy()
        initkwargs.update(kwargs)
        return self.drive_api_class(self.http, self.data_path, **initkwargs)

    @cached_property
    def drive(self):
        return self.get_drive_api()

    def get_files_api(self, **kwargs):
        initkwargs = self.files_initkwargs.copy()
        initkwargs.update(kwargs)
        return self.files_api_class(self, **initkwargs)

    @cached_property
    def files(self):
        return self.get_files_api()
