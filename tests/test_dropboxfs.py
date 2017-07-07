"""DropboxFS tests."""
import datetime
import dropbox
import pytz
import random
import requests
import six
import string
import time
import traceback
import unittest

from mock.mock import patch
from mock.mock import Mock
from mock.mock import PropertyMock

from dropbox.files import (
    CreateFolderError,
    DeletedMetadata,
    DeleteError,
    DownloadError,
    FileMetadata,
    FolderMetadata,
    GetMetadataError,
    ListFolderError,
    ListFolderResult,
    LookupError,
    RelocationError,
    UploadError,
    WriteConflictError,
    WriteError,
)
from dropboxfs import (
    CACHE_TTL,
    CacheItem,
    ChunkedReader,
    ContextManagerStream,
    DropboxCache,
    DropboxClient,
    DropboxFS,
    INFO_TIMEZONE,
    MAX_BUFFER,
    SpooledWriter,
)
from fs.base import NoDefaultMeta
from fs.errors import (
    DestinationExistsError,
    RemoteConnectionError,
    ResourceInvalidError,
    ResourceNotFoundError,
)


class TestSpooledWriter(unittest.TestCase):
    """Test SpooledWriter."""

    def setUp(self):
        client = Mock(spec=DropboxClient)
        self.writer = SpooledWriter(client, '/file1.txt')

    def test_len(self):
        """Test getting the amount written of the file."""
        self.assertEqual(0, len(self.writer))

    def test_write(self):
        """Test writing to the file."""
        self.writer.write('123')

        self.assertEqual(3, len(self.writer))

        self.writer.write(
            ''.join(random.choice(string.lowercase) for x in range(MAX_BUFFER)))

        self.assertEqual(3 + MAX_BUFFER, len(self.writer))

    def test_close(self):
        """Test closing the file."""
        self.writer.close()

        self.assertEqual(1, self.writer.client.files_upload.call_count)
        self.assertIsInstance(
            self.writer.client.files_upload.call_args[0][0],
            six.binary_type)

class TestChunkedReader(unittest.TestCase):
    """Test ChunkedReader."""

    def setUp(self):
        response = Mock(spec=requests.Response)
        response.raw = Mock(
            spec=requests.packages.urllib3.response.HTTPResponse)
        response.raw.closed = False
        response.raw.getheader.return_value = 1028
        client = Mock(spec=DropboxClient)
        client.files_download.return_value = ({}, response)
        self.reader = ChunkedReader(client, '/file1.txt')

    def test_enter(self):
        """Test enter."""
        self.assertEqual(self.reader, self.reader.__enter__())

    def test_exit(self):
        """Test exit."""
        self.reader.__exit__()

        self.assertTrue(self.reader.closed)

    def test_getattr(self):
        """Test getattr."""
        self.assertFalse(self.reader.__getattr__('closed'))

    def test_client_error(self):
        """Test creating the reader with a client error."""
        download_error = DownloadError(tag='other')
        client = Mock(spec=DropboxClient)
        client.files_download.side_effect = dropbox.exceptions.ApiError(
            '1', download_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            ChunkedReader(client, '/files')

    def test_len(self):
        """Test getting the size of the file."""
        self.assertEqual(1028, len(self.reader))

    def test_iter(self):
        """Test getting the iter of the file."""
        self.assertEqual(self.reader, iter(self.reader))

    def test_seek(self):
        """Test seeking in the file."""
        self.reader.size = 50
        self.reader.seek(10)
        self.assertEqual(10, self.reader.seek_pos)
        self.reader.seek(10, 1)
        self.assertEqual(20, self.reader.seek_pos)
        self.reader.seek(10, 2)
        self.assertEqual(self.reader.size + 10, self.reader.seek_pos)

    def test_tell(self):
        """Test getting the current stream position."""
        self.reader.seek(15)
        self.assertEqual(15, self.reader.tell())

    @patch.object(ChunkedReader, 'read')
    def test_next(self, mock_read):
        """Test getting the next piece of data."""
        mock_read.side_effect = ['123', None]

        self.assertEqual('123', self.reader.next())
        with self.assertRaises(StopIteration) as e:
            self.reader.next()

    def test_read(self):
        """Test reading data from the file."""
        self.reader.r.read.side_effect = [
            '123', '456', '789', 'abc', 'def', 'ghi'
        ]

        data = self.reader.read(3)

        self.assertEqual('123', data)
        self.assertEqual(3, self.reader.seek_pos)
        self.assertEqual(3, self.reader.pos)

        data = self.reader.read()

        self.assertEqual('456', data)
        self.assertEqual(1028, self.reader.seek_pos)
        self.assertEqual(1028, self.reader.pos)

        self.reader.seek(64)
        data = self.reader.read(3)

        self.assertEqual('abc', data)
        self.assertEqual(67, self.reader.seek_pos)
        self.assertEqual(67, self.reader.pos)

        self.reader.seek(128)
        data = self.reader.read(3)

        self.assertEqual('ghi', data)
        self.assertEqual(131, self.reader.seek_pos)
        self.assertEqual(131, self.reader.pos)

        self.reader.r.closed = True
        data = self.reader.read()
        self.assertTrue(self.reader.closed)
        self.assertEqual('', data)

    def test_readline(self):
        """Test reading a line of the file."""
        with self.assertRaises(NotImplementedError) as e:
            self.reader.readline()

    def test_readlines(self):
        """Test reading a list of lines of the file."""
        with self.assertRaises(NotImplementedError) as e:
            self.reader.readlines()

    def test_writable(self):
        """Test if file is writeable."""
        self.assertFalse(self.reader.writable())

    def test_writelines(self):
        """Test writing a list of lines to the file."""
        with self.assertRaises(NotImplementedError) as e:
            self.reader.writelines([])

    def test_close(self):
        """Test closing the file."""
        self.reader.close()
        self.assertTrue(self.reader.closed)


class TestCacheItem(unittest.TestCase):
    """Test CacheItem."""

    def setUp(self):
        self.item = CacheItem(timestamp=time.time() - CACHE_TTL)

    def test_add_child(self):
        """Test adding children."""
        self.item.add_child('child1')

        self.assertEqual(1, len(self.item.children))
        self.assertEqual('child1', self.item.children[0])

        self.item.add_child('child2')

        self.assertEqual(2, len(self.item.children))
        self.assertEqual('child1', self.item.children[0])
        self.assertEqual('child2', self.item.children[1])

    def test_del_child(self):
        """Test deleting children."""
        self.item.del_child('child1')

        self.item.add_child('child1')
        self.item.del_child('child2')
        self.item.del_child('child1')

    def test_renew(self):
        """Test renewing an item."""
        self.assertTrue(self.item.expired)

        self.item.renew()

        self.assertFalse(self.item.expired)


class TestDropboxCache(unittest.TestCase):
    """Test DropboxCache."""

    def setUp(self):
        self.cache = DropboxCache()

    def test_set(self):
        """Test setting an item."""
        self.cache.set('/files', {})
        self.cache.set('/files/file.txt', {})

        self.assertEqual(2, len(self.cache))
        self.assertEqual(1, len(self.cache.get('/files').children))
        self.assertEqual('file.txt', self.cache.get('/files').children[0])

    def test_pop(self):
        """Test poping an item."""
        self.cache.set('/files', {})
        self.cache.set('/files/file.txt', {})

        self.cache.pop('/files/file.txt')

        self.assertEqual(1, len(self.cache))


class TestDropboxFS(unittest.TestCase):
    """Test DropboxFS interface."""

    def setUp(self):
        self.fs = DropboxFS('123')

    def test_str(self):
        """Test __str__ method."""
        self.assertEqual('<DropboxFS: >', str(self.fs))

    def test_unicode_str(self):
        """Test unicode __str__ method."""
        self.assertEqual(u'<DropboxFS: >', unicode(self.fs))

    def test_getmeta(self):
        """Test get meta."""
        self.assertEqual('virtual/dropbox', self.fs.getmeta('mime_type'))

    @patch.object(dropbox.Dropbox, 'files_download')
    def test_open_read(self, mock_download):
        """Test opening a file for read."""
        response = Mock(spec=requests.Response)
        response.raw = Mock(
            spec=requests.packages.urllib3.response.HTTPResponse)
        response.raw.getheader.return_value = 0
        mock_download.return_value = ({}, response)

        reader = self.fs.open('/file.txt')

        self.assertIsInstance(reader, ChunkedReader)

    def test_open_write(self):
        """Test opening a file for write."""
        writer = self.fs.open('/file.txt', 'w')

        self.assertIsInstance(writer, SpooledWriter)

    @patch.object(DropboxFS, 'open')
    def test_getcontents(self, mock_open):
        """Test downloading a file."""
        mock_open.return_value.read.return_value = '123'

        data = self.fs.getcontents('/file.txt')

        self.assertEqual('123', data)

    @patch.object(dropbox.Dropbox, 'files_upload')
    def test_setcontents(self, mock_upload):
        """Test uploading a file."""
        mock_upload.return_value = {}

        try:
            self.fs.setcontents('/file.txt', '123')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_upload')
    def test_setcontents_error(self, mock_upload):
        """Test uploading a file with an error."""
        upload_error = UploadError(tag='other')
        mock_upload.side_effect = dropbox.exceptions.ApiError(
            '1', upload_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.setcontents('/file.txt', '123')

    def test_desc(self):
        """Test description."""
        self.assertEqual('/files in Dropbox', self.fs.desc('/files'))

    def test_getsyspath(self):
        """Test get syspath."""
        self.assertEqual('/files', self.fs.getsyspath('files'))

    def test_getsyspath_none(self):
        """Test get syspath allow none."""
        self.assertIsNone(self.fs.getsyspath('files', True))

    @patch.object(DropboxFS, 'getinfo')
    def test_isdir(self, mock_getinfo):
        """Test if a directory."""
        mock_getinfo.return_value = {'isdir': True}

        isdir = self.fs.isdir('/files')

        self.assertTrue(isdir)

    @patch.object(DropboxFS, 'getinfo')
    def test_isdir_false(self, mock_getinfo):
        """Test if not a directory."""
        mock_getinfo.return_value = {'isdir': False}

        isdir = self.fs.isdir('/file.txt')

        self.assertFalse(isdir)

    @patch.object(DropboxFS, 'getinfo')
    def test_isdir_does_not_exist(self, mock_getinfo):
        """Test if not a directory when it does not exist."""
        mock_getinfo.side_effect = ResourceNotFoundError()

        isdir = self.fs.isdir('/files')

        self.assertFalse(isdir)

    @patch.object(DropboxFS, 'getinfo')
    def test_isfile(self, mock_getinfo):
        """Test if a file."""
        mock_getinfo.return_value = {'isdir': False}

        isfile = self.fs.isfile('/file.txt')

        self.assertTrue(isfile)

    @patch.object(DropboxFS, 'getinfo')
    def test_isfile_false(self, mock_getinfo):
        """Test if not a file."""
        mock_getinfo.return_value = {'isdir': True}

        isfile = self.fs.isfile('/files')

        self.assertFalse(isfile)

    @patch.object(DropboxFS, 'getinfo')
    def test_isfile_does_not_exist(self, mock_getinfo):
        """Test if not a file when it does not exist."""
        mock_getinfo.side_effect = ResourceNotFoundError()

        isfile = self.fs.isfile('/file.txt')

        self.assertFalse(isfile)

    @patch.object(DropboxFS, 'getinfo')
    def test_exists(self, mock_getinfo):
        """Test file exists."""
        mock_getinfo.return_value = {}

        exists = self.fs.exists('/file.txt')

        self.assertTrue(exists)

    @patch.object(DropboxFS, 'getinfo')
    def test_exists_false(self, mock_getinfo):
        """Test file does not exist."""
        mock_getinfo.side_effect = ResourceNotFoundError()

        exists = self.fs.exists('/file.txt')

        self.assertFalse(exists)

    @patch.object(CacheItem, 'expired', new_callable=PropertyMock)
    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    @patch.object(dropbox.Dropbox, 'files_list_folder')
    def test_listdir(self, mock_list, mock_metadata, mock_expired):
        """Test listing a directory."""
        file1 = Mock(spec=FileMetadata)
        file1.name = 'file1.txt'
        file2 = Mock(spec=FileMetadata)
        file2.name = 'file2.txt'
        folder1 = Mock(spec=FolderMetadata)
        folder1.name = 'more_files'
        entries = [
            file1,
            file2,
            folder1,
            Mock(spec=DeletedMetadata),
        ]
        mock_metadata.return_value = Mock(FolderMetadata)
        mock_list.side_effect = [
            ListFolderResult(entries=entries),
            ListFolderResult(entries=[]),
            ListFolderResult(entries=[Mock(FolderMetadata)]),
            ListFolderResult(entries=[Mock(FolderMetadata)]),
        ]
        mock_expired.side_effect = [False, False, True]

        children = self.fs.listdir('/files')

        self.assertIsInstance(children, list)
        self.assertEqual(3, len(children))

        # Check that it cached the result
        children = self.fs.listdir('/files')

        self.assertEqual(1, mock_metadata.call_count)
        self.assertEqual(1, mock_list.call_count)
        self.assertIsInstance(children, list)
        self.assertEqual(3, len(children))
        self.assertEqual('file1.txt', children[0])
        self.assertEqual('file2.txt', children[1])
        self.assertEqual('more_files', children[2])

        self.fs.listdir('/folder')

        # Check that it cached the result but still updates with no children
        children = self.fs.listdir('/folder')

        self.assertEqual(3, mock_metadata.call_count)
        self.assertEqual(3, mock_list.call_count)
        self.assertEqual(1, len(children))

        # Check that it cached the result but still updates since it expired
        children = self.fs.listdir('/folder')

        self.assertEqual(4, mock_metadata.call_count)
        self.assertEqual(4, mock_list.call_count)
        self.assertEqual(1, len(children))

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    @patch.object(dropbox.Dropbox, 'files_list_folder')
    def test_listdir_root(self, mock_list, mock_metadata):
        """Test listing the root directory."""
        entries = [
            Mock(spec=FileMetadata),
            Mock(spec=FolderMetadata),
        ]
        mock_list.side_effect = [
            dropbox.exceptions.BadInputError(
                1, 'Specify the root folder as an empty string'),
            ListFolderResult(entries=entries)
        ]
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'The root folder is unsupported')

        children = self.fs.listdir('/')

        self.assertIsInstance(children, list)
        self.assertEqual(2, len(children))

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    @patch.object(dropbox.Dropbox, 'files_list_folder')
    def test_listdir_root_error(self, mock_list, mock_metadata):
        """Test listing the root directory with an error."""
        list_error = ListFolderError(tag='other')
        mock_list.side_effect = [
            dropbox.exceptions.BadInputError(
                1, 'Specify the root folder as an empty string'),
            dropbox.exceptions.ApiError(
                1, list_error, 'message', '')
        ]
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'The root folder is unsupported')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.listdir('/')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    @patch.object(dropbox.Dropbox, 'files_list_folder')
    def test_listdir_root_bad_input(self, mock_list, mock_metadata):
        """Test listing the root directory with bad input."""
        mock_list.side_effect = dropbox.exceptions.BadInputError(1, 'Bad path')
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'The root folder is unsupported')

        with self.assertRaises(dropbox.exceptions.BadInputError) as e:
            self.fs.listdir('/')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_listdir_not_dir(self, mock_metadata):
        """Test listing something not a directory."""
        metadata = FileMetadata(
            name=u'big-file.pdf',
            id=u'id:QsjEAx6f1gAAAAAAAAAAMQ',
            client_modified=datetime.datetime(2017, 3, 6, 15, 44, 28),
            server_modified=datetime.datetime(2017, 6, 19, 16, 24, 12),
            rev=u'6854fd53c4',
            size=957694,
            path_lower=u'/big-file.pdf',
            path_display=u'/big-file.pdf',
            parent_shared_folder_id=None,
            media_info=None,
            sharing_info=None,
            property_groups=None,
            has_explicit_shared_members=None,
            content_hash=u'9e9b314b4df30cf733a6d35a7a8b3aa853eee3b7e78d056b2c2b4d460a331eff'
        )
        mock_metadata.side_effect = [Mock(FileMetadata), metadata]

        with self.assertRaises(ResourceInvalidError) as e:
            self.fs.listdir('file.txt')

        self.fs.getinfo('/big-file.pdf')

        with self.assertRaises(ResourceInvalidError) as e:
            self.fs.listdir('big-file.pdf')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_listdir_bad_input(self, mock_metadata):
        """Test listing a directory with bad input."""
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'Bad path')

        with self.assertRaises(dropbox.exceptions.BadInputError) as e:
            self.fs.listdir('/files')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_listdir_error(self, mock_metadata):
        """Test listing a directory with an error."""
        lookup_error = LookupError(tag='not_found')
        metadata_error = GetMetadataError(tag='path', value=lookup_error)
        mock_metadata.side_effect = dropbox.exceptions.ApiError(
            '1', metadata_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.listdir('/files')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    @patch.object(dropbox.Dropbox, 'files_list_folder')
    def test_listdir_error_listing(self, mock_list, mock_metadata):
        """Test listing a directory with an error while listing."""
        mock_metadata.return_value = Mock(spec=FolderMetadata)
        list_error = ListFolderError(tag='other')
        mock_list.side_effect = dropbox.exceptions.ApiError(
            1, list_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.listdir('/files')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_file(self, mock_metadata):
        """Test getting info for a file."""
        metadata = FileMetadata(
            name=u'big-file.pdf',
            id=u'id:QsjEAx6f1gAAAAAAAAAAMQ',
            client_modified=datetime.datetime(2017, 3, 6, 15, 44, 28),
            server_modified=datetime.datetime(2017, 6, 19, 16, 24, 12),
            rev=u'6854fd53c4',
            size=957694,
            path_lower=u'/big-file.pdf',
            path_display=u'/big-file.pdf',
            parent_shared_folder_id=None,
            media_info=None,
            sharing_info=None,
            property_groups=None,
            has_explicit_shared_members=None,
            content_hash=u'9e9b314b4df30cf733a6d35a7a8b3aa853eee3b7e78d056b2c2b4d460a331eff'
        )
        mock_metadata.return_value = metadata

        try:
            info = self.fs.getinfo('/big-file.pdf')
        except Exception, e:
            self.fail(e)

        self.assertIn('isdir', info)
        self.assertIn('isfile', info)
        self.assertIn('modified_time', info)
        self.assertIn('size', info)
        self.assertIn('path', info)
        self.assertFalse(info['isdir'])
        self.assertTrue(info['isfile'])
        self.assertEqual(
            datetime.datetime(2017, 6, 19, 16, 24, 12, tzinfo=pytz.utc),
            info['modified_time'])
        self.assertEqual(
            INFO_TIMEZONE,
            info['modified_time'].tzinfo.zone)
        self.assertEqual(957694, info['size'])
        self.assertEqual('big-file.pdf', info['path'])

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_file_deleted(self, mock_metadata):
        """Test getting info for a file when it was deleted."""
        mock_metadata.return_value = DeletedMetadata(
            name=u'big-file.pdf',
            path_lower=u'/big-file.pdf',
            path_display=u'/big-file.pdf',
            parent_shared_folder_id=None,
        )

        with self.assertRaises(ResourceNotFoundError) as e:
            self.fs.getinfo('/big-file.pdf')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_folder(self, mock_metadata):
        """Test getting info for a folder."""
        metadata = FolderMetadata(
            name=u'files',
            id=u'id:QsjEAx6f1gAAAAAAAAAAMQ',
            path_lower=u'/files',
            path_display=u'/files',
            parent_shared_folder_id=None,
            shared_folder_id=None,
            sharing_info=None,
            property_groups=None,
        )
        mock_metadata.return_value = metadata

        try:
            info = self.fs.getinfo('/files')
        except Exception, e:
            self.fail(e)

        self.assertIn('isdir', info)
        self.assertIn('isfile', info)
        self.assertIn('modified_time', info)
        self.assertIn('size', info)
        self.assertIn('path', info)
        self.assertTrue(info['isdir'])
        self.assertFalse(info['isfile'])
        self.assertIsNone(info['modified_time'])
        self.assertEqual(0, info['size'])
        self.assertEqual('files', info['path'])

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_root(self, mock_metadata):
        """Test getting info for root directory."""
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'The root folder is unsupported')

        try:
            info = self.fs.getinfo('/')
        except Exception, e:
            self.fail(e)

        self.assertIn('isdir', info)
        self.assertIn('isfile', info)
        self.assertIn('modified_time', info)
        self.assertIn('size', info)
        self.assertIn('path', info)
        self.assertTrue(info['isdir'])
        self.assertFalse(info['isfile'])
        self.assertIsNone(info['modified_time'])
        self.assertEqual(0, info['size'])
        self.assertEqual('/', info['path'])

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_folder_does_not_exist(self, mock_metadata):
        """Test getting info for a folder when it does not exist."""
        lookup_error = LookupError(tag='not_found')
        metadata_error = GetMetadataError(tag='path', value=lookup_error)
        mock_metadata.side_effect = dropbox.exceptions.ApiError(
            '1', metadata_error, 'message', '')

        with self.assertRaises(ResourceNotFoundError) as e:
            self.fs.getinfo('/files')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_folder_bad_input(self, mock_metadata):
        """Test getting info for a folder with bad input."""
        mock_metadata.side_effect = dropbox.exceptions.BadInputError(
            1, 'Bad path')

        with self.assertRaises(dropbox.exceptions.BadInputError) as e:
            self.fs.getinfo('/files')

    @patch.object(dropbox.Dropbox, 'files_get_metadata')
    def test_info_folder_another_error(self, mock_metadata):
        """Test getting info for a folder with another error."""
        lookup_error = LookupError(tag='malformed_path')
        metadata_error = GetMetadataError(tag='path', value=lookup_error)
        mock_metadata.side_effect = dropbox.exceptions.ApiError(
            '1', metadata_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.getinfo('/files')

    @patch.object(dropbox.Dropbox, 'files_copy')
    def test_copy(self, mock_copy):
        """Test copying a file."""
        mock_copy.return_value = {}

        try:
            self.fs.copy('/file1.txt', '/file2.txt')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_copy')
    def test_copy_does_not_exist(self, mock_copy):
        """Test copying a file when it does not exist."""
        lookup_error = LookupError(tag='not_found')
        relocation_error = RelocationError(
            tag='from_lookup', value=lookup_error)
        mock_copy.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(ResourceNotFoundError) as e:
            self.fs.copy('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_copy')
    def test_copy_exists(self, mock_copy):
        """Test copying a file when the destination exists."""
        write_conflict_error = WriteConflictError(tag='file')
        write_error = WriteError(tag='conflict', value=write_conflict_error)
        relocation_error = RelocationError(
            tag='to', value=write_error)
        mock_copy.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(DestinationExistsError) as e:
            self.fs.copy('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_copy')
    def test_copy_error(self, mock_copy):
        """Test copying a file with another error."""
        lookup_error = LookupError(tag='not_file')
        relocation_error = RelocationError(
            tag='from_lookup', value=lookup_error)
        mock_copy.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.copy('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_copy')
    def test_copydir(self, mock_copy):
        """Test copying a directory."""
        mock_copy.return_value = {}

        try:
            self.fs.copydir('/files', '/files2')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_move(self, mock_move):
        """Test moving a file."""
        mock_move.return_value = {}

        try:
            self.fs.move('/file1.txt', '/file2.txt')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_movedir(self, mock_move):
        """Test moving a directory."""
        mock_move.return_value = {}

        try:
            self.fs.movedir('/files', '/files2')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_rename(self, mock_move):
        """Test renaming a file."""
        mock_move.return_value = {}

        try:
            self.fs.rename('/file1.txt', '/file2.txt')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_rename_does_not_exist(self, mock_move):
        """Test renaming a file when it does not exist."""
        lookup_error = LookupError(tag='not_found')
        relocation_error = RelocationError(
            tag='from_lookup', value=lookup_error)
        mock_move.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(ResourceNotFoundError) as e:
            self.fs.rename('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_rename_exists(self, mock_move):
        """Test renaming a file when the destination exists."""
        write_conflict_error = WriteConflictError(tag='file')
        write_error = WriteError(tag='conflict', value=write_conflict_error)
        relocation_error = RelocationError(
            tag='to', value=write_error)
        mock_move.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(DestinationExistsError) as e:
            self.fs.rename('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_move')
    def test_rename_error(self, mock_move):
        """Test renaming a file with another error."""
        lookup_error = LookupError(tag='not_file')
        relocation_error = RelocationError(
            tag='from_lookup', value=lookup_error)
        mock_move.side_effect = dropbox.exceptions.ApiError(
            '1', relocation_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.rename('/file1.txt', '/file2.txt')

    @patch.object(dropbox.Dropbox, 'files_create_folder')
    def test_makedir(self, mock_create_folder):
        """Test creating a folder."""
        mock_create_folder.return_value = {}

        try:
            self.fs.makedir('/files')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_create_folder')
    def test_makedir_exists(self, mock_create_folder):
        """Test creating a folder when it already exists."""
        write_conflict_error = WriteConflictError(tag='folder')
        write_error = WriteError(tag='conflict', value=write_conflict_error)
        create_error = CreateFolderError(tag='path', value=write_error)
        mock_create_folder.side_effect = dropbox.exceptions.ApiError(
            '1', create_error, 'message', '')

        with self.assertRaises(DestinationExistsError) as e:
            self.fs.makedir('/files')

    @patch.object(dropbox.Dropbox, 'files_create_folder')
    def test_makedir_error(self, mock_create_folder):
        """Test creating a folder with another error."""
        write_error = WriteError(tag='insufficient_space')
        create_error = CreateFolderError(tag='path', value=write_error)
        mock_create_folder.side_effect = dropbox.exceptions.ApiError(
            '1', create_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.makedir('/files')

    @patch.object(dropbox.Dropbox, 'files_delete')
    def test_remove(self, mock_delete):
        """Test deleting a file."""
        mock_delete.return_value = {}

        try:
            self.fs.remove('/file.txt')
        except Exception, e:
            self.fail(e)

    @patch.object(dropbox.Dropbox, 'files_delete')
    def test_remove_does_not_exist(self, mock_delete):
        """Test deleting a file when it does not exist."""
        lookup_error = LookupError(tag='not_found')
        delete_error = DeleteError(tag='path_lookup', value=lookup_error)
        mock_delete.side_effect = dropbox.exceptions.ApiError(
            '1', delete_error, 'message', '')

        with self.assertRaises(ResourceNotFoundError) as e:
            self.fs.remove('/file.txt')

    @patch.object(dropbox.Dropbox, 'files_delete')
    def test_remove_error(self, mock_delete):
        """Test deleting a file with another error."""
        lookup_error = LookupError(tag='not_file')
        delete_error = DeleteError(tag='path_lookup', value=lookup_error)
        mock_delete.side_effect = dropbox.exceptions.ApiError(
            '1', delete_error, 'message', '')

        with self.assertRaises(RemoteConnectionError) as e:
            self.fs.remove('/file.txt')

    @patch.object(dropbox.Dropbox, 'files_delete')
    def test_removedir(self, mock_delete):
        """Test deleting a directory."""
        mock_delete.return_value = {}

        try:
            self.fs.removedir('/files')
        except Exception, e:
            self.fail(e)
