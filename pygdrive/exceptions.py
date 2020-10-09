import errno
import os


class GoogleDriveFileExceptionMixin:
    DEFAULT_ERRNO = None
    DEFAULT_STRERROR = ''

    def __init__(self, *args):
        (eno, msg), rest = (args + (None, None))[:2], args[2:]
        super().__init__(*(eno or self.DEFAULT_ERRNO, msg or self.DEFAULT_STRERROR) + rest)


class GoogleDriveFileNotFoundError(GoogleDriveFileExceptionMixin, FileNotFoundError):
    DEFAULT_ERRNO = errno.ENOENT
    DEFAULT_STRERROR = os.strerror(DEFAULT_ERRNO)


class GoogleDriveFileExistsError(GoogleDriveFileExceptionMixin, FileExistsError):
    DEFAULT_ERRNO = errno.EEXIST
    DEFAULT_STRERROR = os.strerror(DEFAULT_ERRNO)


class GoogleDrivePermissionError(GoogleDriveFileExceptionMixin, PermissionError):
    DEFAULT_ERRNO = errno.EPERM
    DEFAULT_STRERROR = os.strerror(DEFAULT_ERRNO)
