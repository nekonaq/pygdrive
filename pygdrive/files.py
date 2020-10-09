import dateutil.parser
import io
import json
import logging
import mimetypes
import os
import re

from googleapiclient.errors import HttpError
from googleapiclient.http import (
    MediaFileUpload,
    MediaIoBaseUpload,
    MediaIoBaseDownload,
)

from .exceptions import (
    GoogleDriveFileNotFoundError,
    GoogleDriveFileExistsError,
    GoogleDrivePermissionError,
)


class FilesAPIWrapper:
    # - 末尾の連続スラッシュを除去する。
    # - 中間の連続スラッシュをひとつにする。
    #
    # NOTE: 中間の連続するスラッシュは os.path.split() が無視するのでそのままでも大丈夫だが、
    # 先頭の連続スラッシュをひとつにするついでに行うようにしてある。
    #
    RE_PATH_DUP_SLASH = re.compile('([^{0}]){0}+$|({0}){0}+'.format(os.path.sep))

    # query の問い合わせ結果に次のフィールドを必ず含める
    FIELDS_DEFAULTS = {'id', 'kind', 'mimeType', 'name', 'parents'}

    UNKNOWN_MIMETYPE = "application/octet-stream"
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
    FOLDER_MIME_TYPE_QUERY = "mimeType='{}'".format(FOLDER_MIME_TYPE)


    UPLOAD_CHUNKSIZE = 1024 * 512


    def __init__(self, client, logger=None):
        # self.client = client
        self.drive = client.drive
        self.service = self.drive.service
        self.logger = logger or logging.getLogger(__name__)

    def rsplit_path(self, path):
        path = self.RE_PATH_DUP_SLASH.sub('\\1\\2', path)
        paths = []
        while True:
            head, tail = os.path.split(path)
            if not tail:
                # under root
                return ({'name': head}, paths)

            if head == os.path.sep:
                # under root
                return ({'name': tail, 'parent_id': 'root'}, paths)

            if not head:
                # no parent
                return ({'name': tail}, paths)

            paths.append(tail)
            path = head

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> gc.files.rsplit_path('')
    ({'name': ''}, [])

    >>> gc.files.rsplit_path('/')
    ({'name': '/'}, [])

    >>> gc.files.rsplit_path('//')
    >>> gc.files.rsplit_path('///')

    >>> gc.files.rsplit_path('hh')
    ({'name': 'hh'}, [])

    >>> gc.files.rsplit_path('/hh')
    ({'name': 'hh', 'parent_id': 'root'}, [])

    >>> gc.files.rsplit_path('//hh')
    >>> gc.files.rsplit_path('///hh')

    >>> gc.files.rsplit_path('/usr/local/bin')
    ({'name': 'usr', 'parent_id': 'root'}, ['bin', 'local'])

    >>> gc.files.rsplit_path('///usr/local/bin')
    >>> gc.files.rsplit_path('///usr//local/bin//')
    >>> gc.files.rsplit_path('///usr///local/bin///')
    """

    def stat(self, path, fields=None):
        stat_params, rest = self.rsplit_path(path)
        try:
            while len(rest) >= 1:
                stat = self._stat(**stat_params, q=self.FOLDER_MIME_TYPE_QUERY)
                stat_params = {
                    'name': rest.pop(),
                    'parent_id': stat['id'],
                }

            if rest:
                stat_params = {
                    'name': rest.pop(),
                    'parent_id': stat['id'],
                }

            return self._stat(**stat_params, fields=fields)
        except GoogleDriveFileNotFoundError as err:
            err.filename = path
            raise err

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> gc.files.stat('')
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: ''

    >>> gc.files.stat('/')                 # root
    {'kind': 'drive#file', 'id': '0ACAD_FCe84GlUk9PVA', 'name': 'My Drive',...}

    >>> gc.files.stat('quickstart')        # has no root
    {'kind': 'drive#file', 'id': '1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE', 'name': 'quickstart',...}

    >>> gc.files.stat('/quickstart')       # under root
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: '/quickstart'

    >>> gc.files.stat('/test sheet')
    {'kind': 'drive#file', 'id': '1pJ0aXfEQGhyUM3qV8dfAa0Ktz3jnGilovx6IITXBD1M', 'name': 'test sheet',...}

    >>> gc.files.stat('test sheet')
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: 'test sheet'

    >>> gc.files.stat('quickstart/test sheet')
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: 'quickstart/test sheet'

    >>> gc.files.stat('quickstart/quicktest/test sheet')
    {'kind': 'drive#file', 'id': '17kHFOGGBXrVPAuVXnYxg6YenJPF2TaW-hgETwJ2XSR4', 'name': 'test sheet',...}

    >>> gc.files.stat('/usr/local/bin')
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: '/usr/local/bin'

    >>> gc.files.stat('///usr//local/bin//')
    >>> gc.files.stat('///usr///local/bin///')

    """

    def build_fields_param(self, fields):
        return ','.join(set((fields or 'id').split(',')) | self.FIELDS_DEFAULTS)

    def _stat(self, name, parent_id=None, fields=None, q=None):
        fields = self.build_fields_param(fields)
        # NOTE: この時点で fields は API files.get() が受け付ける形式

        if name == os.path.sep:
            return self.drive._execute_request(self.service.files().get(fileId='root', fields=fields))

        if not name:
            raise GoogleDriveFileNotFoundError(None, None, name)

        qry = [q] if q else []
        qry.append("name='{0}'".format(name))

        if parent_id:
            qry.append("'{0}' in parents".format(parent_id))

        items = []
        try:
            response = self.drive._execute_request(self.service.files().list(
                fields="nextPageToken, incompleteSearch, files({0})".format(fields),
                q=' and '.join(qry),
            ))
            items = response.get('files', [])
        except HttpError as err:
            """
            >>> err
            <HttpError 404 when requesting https://www.googleapis.com/drive/v3/files?q=%27medoovoo%27+in+parents&fields=nextPageToken%2C+files%28parents%2Cid%2Ckind%2Cname%2CmimeType%29&alt=json returned "File not found: .">

            >>> err.args は tuple (err.resp, err.content) と同じ

            >>> json.loads(err.content)
            {'error': {'code': 404,
                       'errors': [{'domain': 'global',
                                   'location': 'fileId',
                                   'locationType': 'parameter',
                                   'message': 'File not found: .',
                                   'reason': 'notFound'}],
                       'message': 'File not found: .'}}

            >>> err.resp
            {'-content-encoding': 'gzip',
             'alt-svc': 'h3-29=":443"; ma=2592000,h3-27=":443"; ma=2592000,h3-T051=":443"; '
                        'ma=2592000,h3-T050=":443"; ma=2592000,h3-Q050=":443"; '
                        'ma=2592000,h3-Q046=":443"; ma=2592000,h3-Q043=":443"; '
                        'ma=2592000,quic=":443"; ma=2592000; v="46,43"',
             'cache-control': 'private, max-age=0',
             'content-length': '240',
             'content-security-policy': "frame-ancestors 'self'",
             'content-type': 'application/json; charset=UTF-8',
             'date': 'Mon, 07 Sep 2020 05:09:52 GMT',
             'expires': 'Mon, 07 Sep 2020 05:09:52 GMT',
             'server': 'GSE',
             'status': '404',
             'transfer-encoding': 'chunked',
             'vary': 'Origin, X-Origin',
             'x-content-type-options': 'nosniff',
             'x-frame-options': 'SAMEORIGIN',
             'x-xss-protection': '1; mode=block'}

            >>> err.uri
            'https://www.googleapis.com/drive/v3/files?q=%27medoovoo%27+in+parents&fields=nextPageToken%2C+files%28parents%2Cid%2Ckind%2Cname%2CmimeType%29&alt=json'

            >>> err.error_details
            ''

            """
            err_content = json.loads(err.content)
            if err_content['error']['code'] == 404:
                raise GoogleDriveFileNotFoundError(
                    err_content['error']['code'],  # errno
                    None,                          # strerror
                    name,                          # filename
                ) from err
            raise

        if not parent_id:
            # parents を持たないものを対象とする
            items = [el for el in items if 'parents' not in el]

        if not items:
            raise GoogleDriveFileNotFoundError(None, None, name)

        return items[0]

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    # マイドライブのルートは '/'
    >>> pp(gc.files._stat('/'))
    {'id': '0ACAD_FCe84GlUk9PVA',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'My Drive'}

    >>> pp(gc.files._stat(''))             # not found
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: ''

    >>> pp(gc.files._stat('root'))	   # この名前では見つからない

    >>> pp(gc.files._stat('My Drive'))	   # この名前では見つからない

    # ルートフォルダ直下にあるファイルは parent_id='root' を指定する
    >>> pp(gc.files._stat('test sheet', parent_id='root'))  # OK

    >>> pp(gc.files._stat('test sheet'))   # parent_id がないものが存在しないので見つからない

    # 共有フォルダは parents を持たない
    >>> pp(gc.files._stat('quickstart'))   # OK
    """

    def exists(self, path):
        try:
            return self.stat(path) is not None
        except GoogleDriveFileNotFoundError:
            pass
        return False

    def iter(self, path, fields=None, q=None):
        qry = [q] if q else []
        if path == os.path.sep:
            qry.append("'root' in parents")
        elif path:
            stat = self.stat(path)
            qry.append("'{0}' in parents".format(stat['id']))
        else:
            # path が空の場合は共有フォルダ一覧を得る
            qry.append('sharedWithMe')

        list_params = {
            'fields': "nextPageToken, incompleteSearch, files({0})".format(self.build_fields_param(fields)),
            'q': ' and '.join(qry),
        }

        while True:
            response = self.drive._execute_request(self.service.files().list(**list_params))
            for item in response.get('files', []):
                yield item

            if 'nextPageToken' not in response:
                break
            list_params['pageToken'] = response['nextPageToken']

        if response.get('incompleteSearch', None):
            self.logger.warning(
                "{cls.__module__}{cls.__name__}.iter(): "
                "file list may be incomplete: {list_params}".format(
                    cls=self.__class__,
                    list_params=list_params,
                )
            )

    def list(self, path, fields=None, q=None):
        return list(self.iter(path, fields=fields, q=q))

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.list(''))
    [{'id': '1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE',
      'kind': 'drive#file',
      'mimeType': 'application/vnd.google-apps.folder',
      'name': 'quickstart'}]

    >>> pp(gc.files.list('/'))
    [{'id': '1-abuzyG1BbRtpS-i3cjxh2eMG1nEYLCshVmb39r0yKY',
      'kind': 'drive#file',
      'mimeType': 'application/vnd.google-apps.spreadsheet',
      'name': 'test sheet',
      'parents': ['0ACAD_FCe84GlUk9PVA']}]

    >>> pp(gc.files.list('quickstart'))
    [{'id': '1IN5znxXHNe04aQF1ALzevunZHoXyABAe',
      'kind': 'drive#file',
      'mimeType': 'application/vnd.google-apps.folder',
      'name': 'quicktest',
      'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']},
     {'id': '1Ir3MvII45O8ssrRJasxdLuAK6u8hra4w',
      'kind': 'drive#file',
      'mimeType': 'text/plain',
      'name': 'connect.txt',
      'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']}]

    >>> pp(gc.files.list('/quickstart'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: '/quickstart'

    >>> pp(gc.files.list('quickstart/quicktest'))
    [{'id': '17kHFOGGBXrVPAuVXnYxg6YenJPF2TaW-hgETwJ2XSR4',
      'kind': 'drive#file',
      'mimeType': 'application/vnd.google-apps.spreadsheet',
      'name': 'test sheet',
      'parents': ['1IN5znxXHNe04aQF1ALzevunZHoXyABAe']}]

    >>> pp(gc.files.list('quickstart/test'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: 'quickstart/test'

    """

    def _mkdir(self, name, parent_id=None, permissions=None, fields=None):
        if not parent_id:
            # NOTE:
            # parent_id が無指定だと同じ名前のフォルダを複数作成してしまう。
            # これを抑制するために os.mkdir('') と同様 "No such file or directory" を raise する。
            raise GoogleDriveFileNotFoundError(None, None, name)

        return self.drive._execute_request(self.service.files().create(
            body={
                'name': name,
                'mimeType': self.FOLDER_MIME_TYPE,
                'parents': [parent_id],
            },
            fields=self.build_fields_param(fields),
        ))

    def get_or_create_folder(self, path, permissions=None):
        stat_params, rest = self.rsplit_path(path)
        while len(rest) >= 1:
            try:
                stat = self._stat(**stat_params, q=self.FOLDER_MIME_TYPE_QUERY)
            except GoogleDriveFileNotFoundError:
                stat = self._mkdir(**stat_params, permissions=permissions)

            stat_params = {
                'name': rest.pop(),
                'parent_id': stat['id'],
            }

        if rest:
            stat_params = {
                'name': rest.pop(),
                'parent_id': stat['id'],
            }

        try:
            stat = self._stat(**stat_params)
        except GoogleDriveFileNotFoundError:
            stat = None

        stat = stat or self._mkdir(**stat_params, permissions=permissions)
        return stat

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.get_or_create_folder(''))  # 親が root でないフォルダは作成できない
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: ''

    >>> pp(gc.files.get_or_create_folder('/'))  # root
    {'id': '0ACAD_FCe84GlUk9PVA',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'My Drive'}

    >>> pp(gc.files.get_or_create_folder('quickstart'))  # has no root; 共有フォルダ
    {'id': '1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'quickstart'}

    >>> pp(gc.files.get_or_create_folder('/quickstart'))
    {'id': '10DVo2pVvhEUCbsMjq8pFzR6EeqY9RkNP',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'quickstart',
     'parents': ['0ACAD_FCe84GlUk9PVA']}

    >>> pp(gc.files.get_or_create_folder('/test folder'))
    {'id': '1zDCCEA5bYhJZPzGuqvHCCWrhaz3uJEj-',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'test folder',
     'parents': ['0ACAD_FCe84GlUk9PVA']}

    >>> pp(gc.files.get_or_create_folder('test folder'))  # 親が root でないフォルダは作成できない
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileNotFoundError: [Errno 404] No such file or directory: 'test folder'

    >>> pp(gc.files.get_or_create_folder('quickstart/quicktest/voom'))
    {'id': '1pAQYH1dg_dzWXBvWT5pFINoK51e9gFU3',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'voom',
     'parents': ['1IN5znxXHNe04aQF1ALzevunZHoXyABAe']}

    >>> pp(gc.files.get_or_create_folder('/usr/local/bin'))
    {'id': '1YIRJARdg9ZU30k0knX8hmkAfUwWO9YBq',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.folder',
     'name': 'bin',
     'parents': ['1qDOi6lfTVia1m7Yr9KAzp6VfrFZjUzXV']}

    >>> pp(gc.files.get_or_create_folder('///usr//local/bin//'))
    >>> pp(gc.files.get_or_create_folder('///usr///local/bin///'))

    """

    def delete(self, path):
        stat = self.stat(path)
        try:
            return self.drive._execute_request(self.service.files().delete(fileId=stat['id']))
        except HttpError as err:
            err_content = json.loads(err.content)
            if err_content['error']['code'] == 403:
                raise GoogleDrivePermissionError(
                    err_content['error']['code'],    # errno
                    err_content['error']['message'],  # strerror
                    path,                             # filename
                ) from err
            raise

    def list_permissions(self, path, **kwargs):
        stat = self.stat(path)
        return self.drive.list_permissions(stat['id'], **kwargs)

    def delete_permissions(self, path, **kwargs):
        stat = self.stat(path)
        return self.drive.delete_permissions(stat['id'], **kwargs)

    def create_permissions(self, path, role, type, **kwargs):
        stat = self.stat(path)
        return self.drive.delete_permissions(stat['id'], role, type, **kwargs)

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.list_permissions('quickstart'))
    [{'deleted': False,
      'displayName': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'emailAddress': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'id': '15260471987937300434',
      'kind': 'drive#permission',
      'role': 'writer',
      'type': 'user'},
     {'deleted': False,
      'displayName': 'Tatsuo Nakajyo',
      'emailAddress': 'feel.nak@gmail.com',
      'id': '06173720797338992906',
      'kind': 'drive#permission',
      'photoLink': 'https://lh3.googleusercontent.com/a-/AOh14Gje241xrmW9xyIBT283RqIvBXVtVOj0gq8U9dnhfg=s64',
      'role': 'owner',
      'type': 'user'}]

    >>> pp(gc.files.list_permissions('quickstart/connect.txt'))
    [{'deleted': False,
      'displayName': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'emailAddress': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'id': '15260471987937300434',
      'kind': 'drive#permission',
      'role': 'writer',
      'type': 'user'},
     {'deleted': False,
      'displayName': 'Tatsuo Nakajyo',
      'emailAddress': 'feel.nak@gmail.com',
      'id': '06173720797338992906',
      'kind': 'drive#permission',
      'photoLink': 'https://lh3.googleusercontent.com/a-/AOh14Gje241xrmW9xyIBT283RqIvBXVtVOj0gq8U9dnhfg=s64',
      'role': 'owner',
      'type': 'user'}]

    >>> pp(gc.files.list_permissions('/'))
    [{'deleted': False,
      'displayName': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'emailAddress': 'gdrive@quickstart-nekonaq.iam.gserviceaccount.com',
      'id': '15260471987937300434',
      'kind': 'drive#permission',
      'role': 'owner',
      'type': 'user'}]

    """

    def rename(self, path, dst_name, fields=None):
        src_stat = self.stat(path)
        try:
            dst_stat = self._stat(name=dst_name, parent_id=src_stat['parents'][0])
        except GoogleDriveFileNotFoundError:
            dst_stat = None

        if dst_stat:
            # すでに存在するファイルと同じ名前にリネームすることはできない。
            raise GoogleDriveFileExistsError(None, None, os.path.join(os.path.split(path)[0], dst_name))

        return self.drive._execute_request(self.service.files().update(
            fileId=src_stat['id'],
            body={
                'name': dst_name,
            },
            fields=self.build_fields_param(fields),
        ))

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.rename('/test sheet', 'foozoo'))
    {'id': '1-abuzyG1BbRtpS-i3cjxh2eMG1nEYLCshVmb39r0yKY',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.spreadsheet',
     'name': 'foozoo',
     'parents': ['0ACAD_FCe84GlUk9PVA']}

    >>> pp(gc.files.rename('/foozoo', 'foozoo'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: '/foozoo'

    """

    def move(self, path, dst_path, fields=None):
        """
        :param str path: 元のファイルのパス
        :param str dst_path: 移動先フォルダのパス

        NOTE: 元のファイルの親フォルダは dst_path だけになる。
        """
        src_stat = self.stat(path)
        dst_stat = self.stat(dst_path)

        try:
            test_stat = self._stat(src_stat['name'], parent_id=dst_stat['id'])
        except GoogleDriveFileNotFoundError:
            test_stat = None

        if test_stat:
            # 移動先フォルダに同名のファイルがあってはならない。
            # NOTE: 「元のファイルと同じフォルダに移動することはできない」制限もこれでOK。
            raise GoogleDriveFileExistsError(None, None, os.path.join(dst_path, os.path.split(path)[-1]))

        return self.drive._execute_request(self.service.files().update(
            fileId=src_stat['id'],
            addParents=dst_stat['id'],
            removeParents=','.join(src_stat['parents']),
            fields=self.build_fields_param(fields),
        ))

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.move('/test sheet', 'quickstart'))
    {'id': '1eM5UpvuehZRQ_st8LNp99F9hnJWGeSiMyq4yYcuvS1E',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.spreadsheet',
     'name': 'test sheet',
     'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']}

    >>> pp(gc.sheet.create('test sheet'))
    ...

    >>> pp(gc.files.move('quickstart/test sheet', '/'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: '/test sheet'

    >>> pp(gc.files.move('/test sheet', 'quickstart'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: 'quickstart/test sheet'

    >>> pp(gc.files.move('/test sheet', '/'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: '/test sheet'

    """

    def copy(self, path, dst_path, fields=None):
        """
        :param str path: 元のファイルのパス
        :param str dst_path: コピー先フォルダのパス

        NOTE: 元のファイルの親フォルダは dst_path だけになる。
        """
        src_stat = self.stat(path)
        dst_stat = self.stat(dst_path)

        try:
            test_stat = self._stat(src_stat['name'], parent_id=dst_stat['id'])
        except GoogleDriveFileNotFoundError:
            test_stat = None

        if test_stat:
            # コピー先フォルダに同名のファイルがあってはならない。
            # NOTE: 「元のファイルと同じフォルダにコピーすることはできない」制限もこれでOK。
            raise GoogleDriveFileExistsError(None, None, os.path.join(dst_path, os.path.split(path)[-1]))

        return self.drive._execute_request(self.service.files().copy(
            fileId=src_stat['id'],
            body={
                'parents': [dst_stat['id']],
            },
            fields=self.build_fields_param(fields),
        ))

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.copy('quickstart/connect.txt', '/'))
    {'id': '1J60_XZHkUyaEDg62jBpqycucIIizF8uA',
     'kind': 'drive#file',
     'mimeType': 'text/plain',
     'name': 'connect.txt',
     'parents': ['0ACAD_FCe84GlUk9PVA']}

    >>> pp(gc.files.copy('quickstart/connect.txt', '/'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: '/connect.txt'

    >>> pp(gc.files.copy('quickstart/connect.txt', 'quickstart'))
    Traceback (most recent call last):
    ...
    pygdrive.exceptions.GoogleDriveFileExistsError: [Errno 17] File exists: 'quickstart/connect.txt'

    """

    def create(self, path, mimetype=None, media_body=None, permissions=None, fields=None):
        try:
            test_stat = self.stat(path)
        except GoogleDriveFileNotFoundError:
            test_stat = None

        if test_stat:
            raise GoogleDriveFileExistsError(None, None, path)

        folder, name = os.path.split(path)
        dst_stat = self.get_or_create_folder(folder)

        create_params = {
            'body': {
                'name': name,
                'mimeType': mimetype,
                'parents': [dst_stat['id']],
            },
            'fields': self.build_fields_param(fields),
        }

        if media_body:
            create_params['media_body'] = media_body

        return self.drive._execute_request(self.service.files().create(**create_params))

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.create('quickstart/sheet-x', mimetype='application/vnd.google-apps.spreadsheet'))
    {'id': '1euG0Thom1oOQvAqf1SDWgM1DrOAxHWzmPpkklESVIAE',
     'kind': 'drive#file',
     'mimeType': 'application/vnd.google-apps.spreadsheet',
     'name': 'sheet-x',
     'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']}

    """

    def upload(self, path, fh, mimetype=None, permissions=None, fields=None):
        media_body = MediaIoBaseUpload(
            fh,
            mimetype=mimetype or mimetypes.guess_type(path)[0] or self.UNKNOWN_MIMETYPE,
            chunksize=self.UPLOAD_CHUNKSIZE,
            resumable=True,
        )
        return self.create(
            path,
            media_body=media_body,
            permissions=permissions,
            fields=fields,
        )

    def upload_from_file(self, path, filename, mimetype=None, permissions=None, fields=None):
        media_body = MediaFileUpload(
            filename,
            mimetype=mimetype,             # None の場合は元ファイル名から判断する
            chunksize=self.UPLOAD_CHUNKSIZE,
            resumable=True,
        )
        return self.create(
            path,
            media_body=media_body,
            permissions=permissions,
            fields=fields,
        )

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> pp(gc.files.upload_from_file('quickstart/img.jpg', '/var/tmp/taiyaki.jpg'))
    {'id': '15VjAHPLTAIQjOo6aCYDTSKQFseOzoH2k',
     'kind': 'drive#file',
     'mimeType': 'image/jpeg',
     'name': 'img.jpg',
     'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']}

    >>> import io
    >>> fh = io.BytesIO(b'who are you\nme me')
    >>> pp(gc.files.upload('quickstart/msg.txt', fh))
    {'id': '1hMJZTfDq6NRG3spKkl68u0rE1gccy0L3',
     'kind': 'drive#file',
     'mimeType': 'text/plain',
     'name': 'msg.txt',
     'parents': ['1hjAktu_nxbU0xnMMp2Ppl3Q6yMYw5XXE']}

    """

    def get_media_content(self, path, fh):
        stat = self.stat(path)
        req = self.service.files().get_media(fileId=stat['id'])
        return MediaIoBaseDownload(fh, req)

    def download(self, path):
        fh = io.BytesIO()
        download = self.get_media_content(path, fh)
        # :type download: MediaIoBaseDownload
        while True:
            progress, done = download.next_chunk()
            # :type progress: MediaDownloadProgress
            if done:
                break
        fh.seek(0)
        return fh

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> fh = gc.files.download('quickstart/msg.txt')
    >>> txt = fh.read()

    >>> for line in gc.files.download('quickstart/msg.txt'):
    ...   print(line)
    ...

    >>> def get_msg_txt():
    ...   with gc.files.download('quickstart/msg.txt') as fh:
    ...     return fh.read()
    ...

    >>> txt = get_msg_txt()

    """

    def download_to_file(self, path, filename):
        with open(filename, 'wb') as fh:
            download = self.get_media_content(path, fh)
            # :type download: MediaIoBaseDownload
            while True:
                progress, done = download.next_chunk()
                # :type progress: MediaDownloadProgress
                if done:
                    break

    """
    >>> from pprint import pprint as pp
    >>> import pygdrive
    >>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

    >>> gc.files.download_to_file('quickstart/msg.txt', '/var/tmp/msg-dl.txt')

    """

    def size(self, path):
        return int(self.stat(path, fields='size').get('size', 0))

    def url(self, path):
        return self.stat(path, fields='webContentLink').get('webContentLink', '')

    def accessed_time(self, path):
        return self.modified_time(path)

    def created_time(self, path):
        value = self.stat(path, fields='createdTime').get('createdTime')
        return value and dateutil.parser.parse(value)

    def modified_time(self, path):
        value = self.stat(path, fields='modifiedTime').get('modifiedTime')
        return value and dateutil.parser.parse(value)
