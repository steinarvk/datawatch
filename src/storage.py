import contextlib
import io
import os.path
import os
import filenames

def _check_filter(value, filt):
    if filt is None:
        return True
    if isinstance(filt, (list, tuple)):
        return value in filt
    raise ValueError("unknown kind of filter: " + repr(filt))

def _manual_check_item(item, version_shard_filter=None, keyhash_filter=None):
    fni = filenames.decode_filename(item)
    if not _check_filter(fni.version_shard, version_shard_filter):
        return False
    if not _check_filter(fni.keyhash, keyhash_filter):
        return False
    return True

def _manual_filter(items, **kwargs):
    return [item for item in items if _manual_check_item(item, **kwargs)]

class Storage(object):
    def list_chunks(self):
        raise NotImplementedError()

    def list_filtered_chunks(self, **kwargs):
        return _manual_filter(self.list_chunks(), **kwargs)

    def write_chunk(self, filename):
        raise NotImplementedError()

    def read_chunk(self, filename):
        raise NotImplementedError()

class InMemoryStorage(Storage):
    def __init__(self, data=None):
        self._data = data or {}

    def list_chunks(self):
        rv = list(self._data)
        rv.sort()
        for x in rv:
            yield x

    @contextlib.contextmanager
    def write_chunk(self, filename):
        f = io.BytesIO()
        try:
            yield f
        except:
            raise
        else:
            self._data[filename] = f.getvalue()
        finally:
            f.close()
        
    @contextlib.contextmanager
    def read_chunk(self, filename):
        f = io.BytesIO(self._data[filename])
        try:
            yield f
        finally:
            f.close()

class LocalFileStorage(Storage):
    def __init__(self, outpath):
        self._outpath = outpath
        self._abspath = os.path.abspath(outpath)
        if not os.path.exists(self._abspath):
            raise ValueError("output path {} does not exist".format(outpath))

    def __repr__(self):
        return "LocalFileStorage({})".format(repr(self._outpath))

    def list_chunks(self):
        paths = [os.path.join(dp, f) for dp, dn, fn in os.walk(self._abspath) for f in fn]
        rv = []
        prefix = self._abspath + "/"
        for path in paths:
            if not path.startswith(prefix):
                raise ValueError("invalid path listed")
            subpath = path[len(prefix):]
            rv.append(subpath)
        return rv

    @contextlib.contextmanager
    def write_chunk(self, filename):
        joined = os.path.abspath(os.path.join(self._abspath, filename))
        if not joined.startswith(self._abspath):
            raise RuntimeError("local target path {} does not seem to end up below {}; bailing out".format(filename, self._abspath))
        parent, _ = os.path.split(joined)
        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(joined):
            raise RuntimeError("file already exists")
        lockfile = joined + ".lock"
        tmpfile = joined + ".tmp"
        try:
            with open(lockfile, "xb") as f:
                with open(tmpfile, "xb") as f:
                    yield f
                os.rename(tmpfile, joined)
        finally:
            if os.path.exists(lockfile):
                os.remove(lockfile)
            if os.path.exists(tmpfile):
                os.remove(tmpfile)
        
    @contextlib.contextmanager
    def read_chunk(self, filename):
        joined = os.path.abspath(os.path.join(self._abspath, filename))
        if not joined.startswith(self._abspath):
            raise RuntimeError("local target path {} does not seem to end up below {}; bailing out".format(filename, self._abspath))
        with open(joined, "rb") as f:
            yield f
