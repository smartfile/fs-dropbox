fs-dropbox
----------

File system for pyFilesystem which uses the dropbox API.






**A Note About Caching**


This library has built in caching. There are times when you will want to disable
caching. A user can change the file on the remote server and our code/cache will
not be notified of the change. One example of where this is an issue is before
you read/download a file from the remote side if you are using cached meta info
to specify the download size.
