import pygsheets.drive


class DriveAPIWrapper(pygsheets.drive.DriveAPIWrapper):


    '''
    def size(self, name):
        """
        Returns the total size, in bytes, of the file specified by name.
        """
        file_data = self._check_file_exists(name) or {}
        return int(file_data.get('size', 0))

    def url(self, name):
        """
        Returns an absolute URL where the file's contents can be accessed
        directly by a Web browser.
        """
        file_data = self._check_file_exists(name) or {}
        return file_data.get('webContentLink')

    def accessed_time(self, name):
        """
        Returns the last accessed time (as datetime object) of the file
        specified by name.
        """
        return self.modified_time(name)

    def created_time(self, name):
        """
        Returns the creation time (as datetime object) of the file
        specified by name.
        """
        file_data = self._check_file_exists(name) or {}
        value = file_data.get('createdTime')
        return value and dateutil.parser.parse(value)

    def modified_time(self, name):
        """
        Returns the last modified time (as datetime object) of the file
        specified by name.
        """
        file_data = self._check_file_exists(name) or {}
        value = file_data.get('modifiedTime')
        return value and dateutil.parser.parse(value)
    '''


"""
>>> import pygdrive
>>> gc = pygdrive.authorize(service_account_file='/app/src.mnscloud/secrets/google/service-account.json')

>>> gc.drive.listdir('')
>>> gc.drive.listdir('quickstart')

>>> gc.drive.exists('')
>>> gc.drive.exists('quickstart')
>>> gc.drive.exists('quickstart/test.txt')

>>> gc.drive.size('quickstart/connect.txt')
>>> gc.drive.created_time('quickstart/connect.txt')
>>> gc.drive.modified_time('quickstart/connect.txt')
>>> gc.drive.url('quickstart/connect.txt')

"""
