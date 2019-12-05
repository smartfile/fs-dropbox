"""
fs.contrib.dropboxfs
========

A FS object that integrates with Dropbox.

"""

import time
import shutil
import optparse
import tempfile
import logging
import copy
import pytz
from collections import UserDict

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.filelike import StringIO

from dropbox import Dropbox
from dropbox import DropboxOAuth2Flow
from dropbox.exceptions import ApiError
from dropbox.exceptions import BadInputError
from dropbox.files import DeletedMetadata
from dropbox.files import FolderMetadata
from dropbox.files import WriteMode

LOGGER = logging.getLogger(__name__)

# Items in cache are considered expired after 5 minutes.
CACHE_TTL = 300
# Max size for spooling to memory before using disk (5M).
MAX_BUFFER = 1024 ** 2 * 5
# Timezone to use for getinfo
INFO_TIMEZONE = 'America/Indiana/Indianapolis'


class ContextManagerStream(object):
    def __init__(self, temp, name):
        self.temp = temp
        self.name = name

    def __getattr__(self, name):
        return getattr(self.temp, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# TODO: these classes can probably be replaced with
# tempfile.SpooledTemporaryFile, however I am unsure at this moment if doing
# so would be bad since it is only available in Python 2.6+.

class SpooledWriter(ContextManagerStream):
    """Spools bytes to a StringIO buffer until it reaches max_buffer. At that
    point it switches to a temporary file."""
    def __init__(self, client, name, max_buffer=MAX_BUFFER):
        self.client = client
        self.max_buffer = max_buffer
        self.bytes = 0
        super(SpooledWriter, self).__init__(StringIO(), name)

    def __len__(self):
        return self.bytes

    def write(self, data):
        if self.temp.tell() + len(data) >= self.max_buffer:
            # We reached the max_buffer size that we want to keep in memory.
            # Switch to an on-disk temp file. Copy what has been written so
            # far to it.
            temp = tempfile.TemporaryFile()
            self.temp.seek(0)
            shutil.copyfileobj(self.temp, temp)
            self.temp = temp
        self.temp.write(data)
        self.bytes += len(data)

    def close(self):
        # Need to flush temporary file (but not StringIO).
        if hasattr(self.temp, 'flush'):
            self.temp.flush()
        self.temp.seek(0)
        self.client.files_upload(
            self.temp.read(),
            self.name,
            mode=WriteMode.overwrite)
        self.temp.close()


class ChunkedReader(ContextManagerStream):
    """ A file-like that provides access to a file with dropbox API"""
    """Reads the file from the remote server as requested.
    It can then satisfy read()."""
    def __init__(self, client, name):
        self.client = client
        try:
            _, response = self.client.files_download(name)
            self.r = response.raw
        except ApiError as e:
            LOGGER.error(e, exc_info=True, extra={'stack': True,})
            raise RemoteConnectionError(opname='get_file', path=name,
                                        details=e)
        self.bytes = int(self.r.getheader('Content-Length'))
        self.name = name
        self.closed = False
        self.pos = 0
        self.seek_pos = 0
        super(ChunkedReader, self).__init__(self.r, name)

    def __len__(self):
        return self.bytes

    def __iter__(self):
        return self

    def seek(self, offset, whence=0):
        """
        Change the stream position to the given byte offset in the file-like
        object.
        """
        if (whence == 0):
            self.seek_pos = offset
        elif (whence == 1):
            self.seek_pos += offset
        elif (whence == 2):
            self.seek_pos = self.size + offset

    def tell(self):
        """ Return the current stream position. """
        return self.seek_pos

    def __next__(self):
        """
        Read the data until all data is read.
        data is empty string when there is no more data to read.
        """
        data = self.read()
        if data is None:
            raise StopIteration()
        return data

    def read(self, amt=None):
        """ Read a piece of the file from dropbox. """
        if not self.r.closed:
            # Do some fake seeking
            if self.seek_pos < self.pos:
                self.r.close()
                _, response = self.client.files_download(self.name)
                self.r = response.raw
                self.r.read(self.seek_pos)
            elif self.seek_pos > self.pos:
                # Read ahead enough to reconcile pos and seek_pos
                self.r.read(self.pos - self.seek_pos)
            self.pos = self.seek_pos

            # Update position pointers
            if amt:
                self.pos += amt
                self.seek_pos += amt
            else:
                self.pos = self.bytes
                self.seek_pos = self.bytes
            return self.r.read(amt)
        else:
            self.close()
            return ''

    def readline(self, size=-1):
        """ Not implemented. Read and return one line from the stream. """
        raise NotImplementedError()

    def readlines(self, hint=-1):
        """
        Not implemented. Read and return a list of lines from the stream.
        """
        raise NotImplementedError()

    def writable(self):
        """ The stream does not support writing. """
        return False

    def writelines(self, lines):
        """ Not implemented. Write a list of lines to the stream. """
        raise NotImplementedError()

    def close(self):
        """
        Flush and close this stream. This method has no effect if the file
        is already closed. As a convenience, it is allowed to call this method
        more than once; only the first call, however, will have an effect.
        """
        # It's a memory leak if self.r not closed.
        if not self.r.closed:
            self.r.close()
        if not self.closed:
            self.closed = True


class CacheItem(object):
    """Represents a path in the cache. There are two components to a path.
    It's individual metadata, and the children contained within it."""
    def __init__(self, metadata=None, children=None, timestamp=None):
        self.metadata = metadata
        self.children = children
        if timestamp is None:
            timestamp = time.time()
        self.timestamp = timestamp

    def add_child(self, name):
        if self.children is None:
            self.children = [name]
        else:
            self.children.append(name)

    def del_child(self, name):
        if self.children is None:
            return
        try:
            i = self.children.index(name)
        except ValueError:
            return
        self.children.pop(i)

    def _get_expired(self):
        if self.timestamp <= time.time() - CACHE_TTL:
            return True
    expired = property(_get_expired)

    def renew(self):
        self.timestamp = time.time()


class DropboxCache(UserDict):
    def set(self, path, metadata):
        self[path] = CacheItem(metadata)
        dname, bname = pathsplit(path)
        item = self.get(dname)
        if item:
            item.add_child(bname)

    def pop(self, path, default=None):
        value = UserDict.pop(self, path, default)
        dname, bname = pathsplit(path)
        item = self.get(dname)
        if item:
            item.del_child(bname)
        return value


class DropboxClient(Dropbox):
    """A wrapper around the official Dropbox client. This wrapper performs
    caching as well as converting errors to fs exceptions."""
    def __init__(self, *args, **kwargs):
        super(DropboxClient, self).__init__(*args, **kwargs)
        self.cache = DropboxCache()

    # Below we split the DropboxClient metadata() method into two methods
    # metadata() and children(). This allows for more fine-grained fetches
    # and caching.

    def metadata(self, path, cache_read=True):
        "Gets metadata for a given path."
        item = self.cache.get(path) if cache_read else None
        if not item or item.metadata is None or item.expired:
            try:
                metadata = super(DropboxClient, self).files_get_metadata(
                    path, include_deleted=False)
            except BadInputError as e:
                # Root folder is unsupported
                if 'The root folder is unsupported' in e.message:
                    metadata = FolderMetadata(name='/', path_display='/')
                else:
                    raise
            except ApiError as e:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    raise ResourceNotFoundError(path)
                LOGGER.error(e, exc_info=True, extra={'stack': True,})
                raise RemoteConnectionError(opname='metadata', path=path,
                                            details=e)
            if isinstance(metadata, DeletedMetadata):
                raise ResourceNotFoundError(path)
            item = self.cache[path] = CacheItem(metadata)
        # Copy the info so the caller cannot affect our cache.
        return copy.deepcopy(item.metadata)

    def children(self, path):
        "Gets children of a given path."
        update = False
        item = self.cache.get(path)
        if item:
            if item.expired:
                update = True
            else:
                if not isinstance(item.metadata, FolderMetadata):
                    raise ResourceInvalidError(path)
            if not item.children:
                update = True
        else:
            update = True
        if update:
            try:
                metadata = super(DropboxClient, self).files_get_metadata(
                    path, include_deleted=False)
            except BadInputError as e:
                # Root folder is unsupported
                if 'The root folder is unsupported' in e.message:
                    metadata = FolderMetadata(name='/', path_display='/')
                else:
                    raise
            except ApiError as e:
                LOGGER.error(e, exc_info=True, extra={'stack': True,})
                raise RemoteConnectionError(opname='metadata', path=path,
                                            details=e)

            if not isinstance(metadata, FolderMetadata):
                raise ResourceInvalidError(path)

            try:
                folder_list = super(DropboxClient, self).files_list_folder(
                    path, include_deleted=False)
            except BadInputError as e:
                # Specify the root folder as an empty string rather than as "/"
                if 'Specify the root folder as an empty string' in e.message:
                    try:
                        folder_list = super(DropboxClient, self).files_list_folder(
                            '', include_deleted=False)
                    except ApiError as e:
                        LOGGER.error(e, exc_info=True, extra={'stack': True,})
                        raise RemoteConnectionError(opname='metadata', path=path,
                                                    details=e)
                else:
                    raise
            except ApiError as e:
                LOGGER.error(e, exc_info=True, extra={'stack': True,})
                raise RemoteConnectionError(opname='metadata', path=path,
                                            details=e)
            children = []
            for child in folder_list.entries:
                if isinstance(child, DeletedMetadata):
                    continue
                children.append(child.name)
                self.cache[child.path_display] = CacheItem(child)
            item = self.cache[path] = CacheItem(metadata, children)

        return item.children

    def files_create_folder(self, path):
        "Add newly created directory to cache."
        try:
            metadata = super(DropboxClient, self).files_create_folder(path)
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_conflict():
                raise DestinationExistsError(path)
            LOGGER.error(e, exc_info=True, extra={'stack': True,})
            raise RemoteConnectionError(opname='file_create_folder', path=path,
                                        details=e)
        self.cache.set(path, metadata)

    def files_copy(self, src, dst):
        try:
            metadata = super(DropboxClient, self).files_copy(src, dst)
        except ApiError as e:
            if e.error.is_from_lookup() and e.error.get_from_lookup().is_not_found():
                raise ResourceNotFoundError(src)
            if e.error.is_to() and e.error.get_to().is_conflict():
                raise DestinationExistsError(dst)
            LOGGER.error(e, exc_info=True, extra={'stack': True,})
            raise RemoteConnectionError(opname='file_copy', path=src,
                                        details=e)
        self.cache.set(dst, metadata)

    def files_move(self, src, dst):
        try:
            metadata = super(DropboxClient, self).files_move(src, dst)
        except ApiError as e:
            if e.error.is_from_lookup() and e.error.get_from_lookup().is_not_found():
                raise ResourceNotFoundError(src)
            if e.error.is_to() and e.error.get_to().is_conflict():
                raise DestinationExistsError(dst)
            LOGGER.error(e, exc_info=True, extra={'stack': True,})
            raise RemoteConnectionError(opname='file_move', path=src,
                                        details=e)
        self.cache.pop(src, None)
        self.cache.set(dst, metadata)

    def files_delete(self, path):
        try:
            super(DropboxClient, self).files_delete(path)
        except ApiError as e:
            if e.error.is_path_lookup() and e.error.get_path_lookup().is_not_found():
                raise ResourceNotFoundError(path)
            raise RemoteConnectionError(opname='file_delete', path=path,
                                        details=e)
        self.cache.pop(path, None)

    def files_upload(self, f, path, mode=WriteMode('add', None)):
        try:
            super(DropboxClient, self).files_upload(f, path, mode)
        except ApiError as e:
            LOGGER.error(e, exc_info=True, extra={'stack': True,})
            raise RemoteConnectionError(opname='put_file', path=path,
                                        details=e)
        self.cache.pop(dirname(path), None)


def create_client(token):
    """Uses token to gain access to the API."""
    return DropboxClient(token)


def metadata_to_info(metadata, localtime=False):
    isdir = isinstance(metadata, FolderMetadata)
    modified_time = getattr(metadata, 'server_modified', None)
    if modified_time:
        modified_time = modified_time.replace(tzinfo=pytz.utc).astimezone(
            pytz.timezone(INFO_TIMEZONE))
    info = {
        'size': getattr(metadata, 'size', 0),
        'isdir': isdir,
        'isfile': not isdir,
        'modified_time': modified_time,
        'path': metadata.name,
    }
    return info


class DropboxFS(FS):
    """A FileSystem that stores data in Dropbox."""

    _meta = {'thread_safe': True,
             'virtual': False,
             'read_only': False,
             'unicode_paths': True,
             'case_insensitive_paths': True,
             'network': True,
             'atomic.setcontents': False,
             'atomic.makedir': True,
             'atomic.rename': True,
             'mime_type': 'virtual/dropbox', }

    def __init__(self, token, localtime=False, thread_synchronize=True):
        """Create an fs that interacts with Dropbox.

        :param token: The access token you received after authorization.
        :param thread_synchronize: set to True (default) to enable thread-safety
        """
        super(DropboxFS, self).__init__(thread_synchronize=thread_synchronize)
        self.client = create_client(token)
        self.localtime = localtime

    def __str__(self):
        return "<DropboxFS: >"

    def __unicode__(self):
        return "<DropboxFS: >"

    @synchronize
    def open(self, path, mode="rb", **kwargs):
        if 'r' in mode:
            return ChunkedReader(self.client, path)
        else:
            return SpooledWriter(self.client, path)

    @synchronize
    def getcontents(self, path, mode="rb"):
        path = abspath(normpath(path))
        return self.open(path, mode).read()

    def setcontents(self, path, data, *args, **kwargs):
        path = abspath(normpath(path))
        self.client.files_upload(data, path, mode=WriteMode.overwrite)

    def desc(self, path):
        return "%s in Dropbox" % path

    def getsyspath(self, path, allow_none=False):
        "Returns a path as the Dropbox API specifies."
        if allow_none:
            return None
        return abspath(normpath(path))

    def isdir(self, path):
        try:
            info = self.getinfo(path)
            return info.get('isdir', False)
        except ResourceNotFoundError:
            return False

    def isfile(self, path):
        try:
            info = self.getinfo(path)
            return not info.get('isdir', False)
        except ResourceNotFoundError:
            return False

    def exists(self, path):
        try:
            self.getinfo(path)
            return True
        except ResourceNotFoundError:
            return False

    def listdir(self, path='', wildcard=None, full=False, absolute=False,
                dirs_only=False, files_only=False):
        path = abspath(normpath(path))
        children = self.client.children(path)
        return self._listdir_helper(path, children, wildcard, full, absolute,
                                    dirs_only, files_only)

    @synchronize
    def getinfo(self, path, cache_read=True):
        path = abspath(normpath(path))
        metadata = self.client.metadata(path, cache_read=cache_read)
        return metadata_to_info(metadata, localtime=self.localtime)

    def copy(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.files_copy(src, dst)

    def copydir(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.files_copy(src, dst)

    def move(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.files_move(src, dst)

    def movedir(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.files_move(src, dst)

    def rename(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.files_move(src, dst)

    def makedir(self, path, recursive=False, allow_recreate=False):
        path = abspath(normpath(path))
        self.client.files_create_folder(path)

    def remove(self, path):
        path = abspath(normpath(path))
        self.client.files_delete(path)

    def removedir(self, path, *args, **kwargs):
        path = abspath(normpath(path))
        self.client.files_delete(path)


def main():  # pragma: no cover
    parser = optparse.OptionParser(prog="dropboxfs",
                                   description="CLI harness for DropboxFS.")
    parser.add_option(
        "-k",
        "--app-key",
        help="Your Dropbox app key.")
    parser.add_option(
        "-s",
        "--app-secret",
        help="Your Dropbox app secret.")
    parser.add_option(
        "-t",
        "--type",
        default='dropbox',
        choices=('dropbox', 'app_folder'),
        help="Your Dropbox app access type.")
    parser.add_option(
        "-a",
        "--token",
        help="Your access token key (if you previously obtained one.")

    (options, args) = parser.parse_args()

    # Can't operate without these parameters.
    if not options.app_key or not options.app_secret:
        parser.error('You must obtain an app key and secret from Dropbox at the following URL.\n\nhttps://www.dropbox.com/developers/apps')

    # Instantiate a client one way or another.
    if not options.token:
        session = {}
        dbx = DropboxOAuth2Flow(
            options.app_key,
            options.app_secret,
            'https://goo.gl/',
            session,
            'dropbox-auth-csrf-token')
        print("Please visit the following URL and authorize this application.\n")
        print(dbx.start())
        print("\nWhen you are done, observe the query parameters from the redirect and press <enter>.")
        input()
        state = input('Please enter the state from the query parameters: ')
        code = input('Please enter the code from the query parameters: ')
        result = dbx.finish({'state': state, 'code': code})
        token = result.access_token
        print('Your access token will be printed below, store it for later use.')
        print('For future accesses, you can pass the --token argument.\n')
        print('Access token:', result.access_token)
        print("\nWhen you are done, please press <enter>.")
        input()
    else:
        token = options.token

    fs = DropboxFS(token)

    print(fs.getinfo('/Public'))
    if fs.exists('/Bar'):
        fs.removedir('/Bar')
    print(fs.listdir('/'))
    fs.makedir('/Bar')
    print(fs.listdir('/'))
    print(fs.listdir('/Foo'))

    filelike = fs.open('/big-file.pdf')
    print(filelike.read(100))
    filelike.seek(100)
    chunk2 = filelike.read(100)
    print(chunk2)
    filelike.seek(200)
    print(filelike.read(100))
    filelike.seek(100)
    chunk2a = filelike.read(100)
    print(chunk2a)
    assert chunk2 == chunk2a
    filelike.close()

if __name__ == '__main__':  # pragma: no cover
    main()
