import methods
import version
import filenames
import storage
import binascii
import collections
import contextlib
import zlib
import os.path
import io
import json
import operator
import codecs

_FORMAT_MAGIC = "datadiff"
_FORMAT_VERSION = "0.0.1"

DatadiffVersionsHeader = collections.namedtuple("DatadiffVersionsHeader", [
    "first_contained_version",
    "last_contained_version",
    "first_known_version",
    "last_contained_version_with_diff",
    "depends_on_external_version",
])

DatadiffHeader = collections.namedtuple("DatadiffHeader", [
    "magic",
    "format_version",
    "software_version",
    "name",
    "nameinfo",
    "key",
    "methods",
    "versioninfo",
])

IncarnationHeader = collections.namedtuple("IncarnationHeader", [
    "version",
    "content_hash",
    "content_length",
])

_HEADER_BASE = {
    "magic": _FORMAT_MAGIC,
    "format_version": _FORMAT_VERSION,
    "methods": methods.ACTIVE_METHODS,
    "software_version": version.VERSION_METADATA,
}

def _make_header(nameinfo, versioninfo, key):
    return DatadiffHeader(
        name=filenames.encode_filename_from_nameinfo(nameinfo),
        nameinfo=nameinfo._asdict(),
        versioninfo=versioninfo._asdict(),
        key=key,
        **_HEADER_BASE)._asdict()

def _pack_bytes(data):
    return binascii.b2a_base64(data).decode("utf-8").strip()

def _unpack_bytes(b64s):
    return binascii.a2b_base64(b64s)

def _boolcount(*bools):
    return len([x for x in bools if x])

def _coerce_to_readflo(x):
    if isinstance(x, bytes):
        return io.BytesIO(x)
    if isinstance(x, str):
        return io.BytesIO(x.encode("utf-8"))
    return x

def _coerce_to_bytes(x):
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode("utf-8")
    return x.read()

class DataIncarnation(object):
    def __init__(self, data, data_version):
        self._ver = data_version
        self._data = data
        self._memo_data_as_unicode = None
        content_hash = methods.compute_content_hash(data)
        self._content_hash_digest = content_hash["digest"]
        self._metadata = IncarnationHeader(
            version=self._ver,
            content_hash=content_hash,
            content_length=len(data),
        )._asdict()
        self._memo = {}

    def same_data_as(self, other):
        if self.content_hash_digest != other.content_hash_digest:
            return False
        return self.data == other.data

    def get_data_as_bytes_or_unicode(self):
        if self._memo_data_as_unicode is not None:
            if self._memo_data_as_unicode is False:
                return self._data
            return self._memo_data_as_unicode
        try:
            decoded = self._data.decode("utf-8")
        except UnicodeDecodeError:
            self._memo_data_as_unicode = False
            return self._data
        self._memo_data_as_unicode = decoded
        return self._memo_data_as_unicode

    def get_data_as_unicode(self):
        rv = self.get_data_as_bytes_or_unicode()
        if isinstance(rv, bytes):
            raise UnicodeDecodeError("unicode decode error cached from earlier")
        return rv

    @property
    def data(self):
        return self._data

    @property
    def data_version(self):
        return self._ver

    @property
    def content_hash_digest(self):
        return self._content_hash_digest

    def _full_content_record(self):
        min_savings = 50
        compressed = zlib.compress(self._data)
        method = "zlib.compress"
        if len(compressed) < (len(self._data) - min_savings):
            return {
                "full_compressed": {
                    "method": method,
                    "data": _pack_bytes(compressed),
                },
            }
        return {
            "full": _pack_bytes(self._data),
        }

    def _content_record_same_as(self, equal_previous_ver):
        assert equal_previous_ver.data == self.data
        return {
            "baseline_version": equal_previous_ver.data_version,
            "unchanged": True,
        }

    def _delta_content_record(self, last):
        if last.data == self.data:
            return self._content_record_same_as(last)
        diff = methods.compute_diff(last.data, self.data)
        full = self._full_content_record()
        full_data = _unpack_bytes(full["full"] if ("full" in full) else full["full_compressed"]["data"])
        if len(diff) > len(full_data):
            return full
        return {
            "baseline_version": last.data_version,
            "diff": {
                "method": methods.ACTIVE_METHODS["diff"],
                "data": _pack_bytes(diff),
            },
        }

    def _content_record(self, last, previous):
        if not last:
            return self._full_content_record()
        try:
            equal_old = previous[self._content_hash_digest]
        except KeyError:
            pass
        else:
            if self.data == equal_old.data:
                return self._content_record_same_as(equal_old)
        k = (last.data_version, last.content_hash_digest)
        try:
            return self._memo[k]
        except KeyError:
            pass
        rv = self._delta_content_record(last)
        self._memo = {k: rv}
        return rv

    def as_record(self, baseline, previous_by_content):
        return {
          "metadata": self._metadata,
          "content": self._content_record(baseline, previous_by_content),
        }

    @staticmethod
    def build_from_record(record, baseline):
        def handle_full(cont):
            return _unpack_bytes(cont)
        def handle_full_compressed(cont):
            if cont["method"] != "zlib.compress":
                raise ValueError("invalid 'compressed' section: unexpected method".format(repr(cont["method"])))
            return zlib.decompress(_unpack_bytes(cont["data"]))
        def handle_diff(cont):
            if cont["method"] != methods.ACTIVE_METHODS["diff"]:
                raise ValueError("invalid or unhandled 'diff' section: unknown method ({}); perhaps from a future version?".format(repr(cont["method"])))
            if not baseline:
                raise ValueError("invalid 'diff' section: missing baseline")
            old_data = baseline.data
            patch_bytes = _unpack_bytes(cont["data"])
            new_data = methods.apply_patch(old_data, patch_bytes)
            return new_data
        def handle_unchanged(cont):
            if cont != True:
                raise ValueError("invalid 'unchanged' section: should be True; was {}".format(repr(cont)))
            if not baseline:
                raise ValueError("invalid 'unchanged' section: missing baseline")
            return baseline.data
        _valid_encodings = {
            "full": handle_full,
            "full_compressed": handle_full_compressed,
            "diff": handle_diff,
            "unchanged": handle_unchanged,
        }
        data_version = record["metadata"]["version"]
        mutdict = dict(record["content"])
        if "baseline_version" in mutdict:
            if (not baseline) or (baseline.data_version != mutdict["baseline_version"]):
                raise ValueError("no baseline provided or wrong baseline provided ({}; wanted {})".format(baseline, mutdict["baseline_version"]))
            del mutdict["baseline_version"]
        if len(mutdict) != 1:
            raise ValueError("invalid content section: expected exactly one encoding method")
        k = list(mutdict)[0]
        try:
            handler = _valid_encodings[k]
        except KeyError:
            raise ValueError("invalid or unknown encoding method {}".format(repr(k)))
        data = handler(mutdict[k])
        inc = DataIncarnation(data, data_version)
        new, old = inc._metadata["content_length"], record["metadata"]["content_length"]
        if new != old:
            raise ValueError("bailing out: data for {} could not be reconstructed to pass length check ({} vs. {})".format(data_version, new, old))
        # XXX respect different hashing methods
        new, old = inc.content_hash_digest, record["metadata"]["content_hash"]["digest"]
        if new != old:
            raise ValueError("bailing out: data for {} could not be reconstructed to pass hash check ({} vs. {})".format(data_version, new, old))
        return inc

class Entry(object):
    def __init__(self, key, versioninfo, dependency_chain_length, incarnations):
        self._key = key
        self._keyhash = methods.compute_key_hash(key)["digest"]
        self._versioninfo = versioninfo
        self._chain_length = dependency_chain_length
        self._incarnations = incarnations
        self._external_last_version = None

    @staticmethod
    def create_initial(key, data, data_version):
        ver = data_version
        vers = DatadiffVersionsHeader(
            first_contained_version=ver,
            last_contained_version=ver,
            first_known_version=ver,
            last_contained_version_with_diff=ver,
            depends_on_external_version=None,
        )
        incarn = [DataIncarnation(data=data, data_version=ver)]
        return Entry(key=key,
          dependency_chain_length=0,
          versioninfo=vers,
          incarnations=incarn)

    @staticmethod
    def _parse_dump_file(reader, handle_record=None, handle_header=None):
        with reader() as f:
            # XXX simplistic implementation
            rv = json.load(f)
            # XXX validate with jsonschema?
            if handle_header:
                handle_header(rv["datawatch"]["header"])
            if handle_record:
                for record in rv["datawatch"]["content"]:
                    handle_record(record)

    @staticmethod
    def _load_from_dump_files(filenames_with_readers, only_from_last_checkpoint=False, full_history=False):
        if _boolcount(only_from_last_checkpoint, full_history) != 1:
            raise ValueError("exactly one read mode must be set (only_from_last_checkpoint or full_history)")
        ctx = {}
        datas = {}
        recs_by_version = {}
        versions_required = set()
        last_with_diff = None
        def ensure_consistent(k, hdr, getter):
            new_value = getter(hdr)
            try:
                last_value = ctx[k]
            except KeyError:
                ctx[k] = new_value
            else:
                if new_value != last_value:
                    raise RuntimeError("inconsistent: file {} has field {} set to {}, which is not the same as the previously loaded key {}".format(hdr["name"], repr(k), new_value, last_value))
        def on_header(header):
            ensure_consistent("key", header, operator.itemgetter("key"))
            ensure_consistent("versioninfo.first_known_version", header, lambda h: h["versioninfo"]["first_known_version"])
            lc = header["versioninfo"]["last_contained_version_with_diff"]
            if not ctx.get("last_with_diff"):
                ctx["last_with_diff"] = lc
            elif lc:
                ctx["last_with_diff"] = max(ctx["last_with_diff"], ctx["last_with_diff"])
        def on_record(rec):
            v = rec["metadata"]["version"]
            recs_by_version[v] = rec
            req = rec["content"].get("baseline_version")
            if req:
                versions_required.add(req)
        filenames_with_readers = list(filenames_with_readers)
        if not filenames_with_readers:
            raise RuntimeError("no files specified")
        fni_with_readers = [(filenames.decode_filename(name), reader) for name, reader in filenames_with_readers]
        stamped_fni_with_readers = [(int(fni.last_version), fni, reader) for fni, reader in fni_with_readers]
        stamped_fni_with_readers.sort()
        fnis_with_readers_by_stamp = {str(v): (fni, reader) for (v, fni, reader) in stamped_fni_with_readers}
        latest = tuple(stamped_fni_with_readers[-1][1:])
        trail = [latest]
        def find_step_by_version(k):
            try:
                return fnis_with_readers_by_stamp[k]
            except KeyError:
                pass
            for _, fni, reader in stamped_fni_with_readers:
                has = int(fni.first_version) <= int(k) <= int(fni.last_version)
                if has:
                    return (fni, reader)
            raise RuntimeError("provided set of {} chunks does not contain a file containing {}".format(len(filenames_with_readers), k))
        while trail[-1][0].depends_on_version:
            k = trail[-1][0].depends_on_version
            next_step = find_step_by_version(k)
            trail.append(next_step)
        if only_from_last_checkpoint:
            to_load = trail
        else:
            assert full_history
            to_load = filenames_with_readers
        n = 0
        for filename, reader in to_load:
            Entry._parse_dump_file(reader, handle_header=on_header, handle_record=on_record)
            n += 1
        if not n:
            raise RuntimeError("no files specified")
        external_versions_req = versions_required - set(recs_by_version)
        # TODO: allow recovery from first possible checkpoint. data before that can't be loaded.
        if len(external_versions_req) > 1:
            rv = list(external_versions_req)
            raise RuntimeError("files do not cover a contiguous set of versions: multiple external versions would be required (forbidden): {}".format(repr(rv)))
        if len(external_versions_req) > 0:
            rv = list(external_versions_req)
            raise RuntimeError("files do not cover a self-contained set of versions: external version would be required (forbidden): {}".format(repr(rv)))
        versionlist = list(recs_by_version)
        versionlist.sort()
        versioninfo = DatadiffVersionsHeader(
            first_contained_version=versionlist[0],
            last_contained_version=versionlist[-1],
            last_contained_version_with_diff=ctx["last_with_diff"],
            first_known_version=ctx["versioninfo.first_known_version"],
            depends_on_external_version=None,
        )
        built_incarnations = []
        built_incarnations_index = {}
        for v in versionlist:
            rec = recs_by_version[v]
            cont = rec["content"]
            try:
                baseline_ver = cont["baseline_version"]
            except KeyError:
                baseline_ver = None
                baseline_inc = None
            if baseline_ver:
                try:
                    baseline_inc = built_incarnations_index[baseline_ver]
                except KeyError:
                    raise RuntimeError("content for {} refers to version {} out of sequence".format(v, baseline_ver))
            new_inc = DataIncarnation.build_from_record(rec, baseline=baseline_inc)
            built_incarnations.append(new_inc)
            built_incarnations_index[v] = new_inc
        return Entry(key=ctx["key"],
          dependency_chain_length=0,
          versioninfo=versioninfo,
          incarnations=built_incarnations)

    def write_dump(self, storage):
        self._write_named_json(storage.write_chunk)

    @staticmethod
    def load_dumps(storage, filenames, **kwargs):
        filenames = list(filenames)
        filename_readers = []
        for fn in filenames:
            def make_contextmanager(captured_fn):
                @contextlib.contextmanager
                def read_this_chunk():
                    with storage.read_chunk(captured_fn) as f:
                        yield f
                return read_this_chunk
            filename_readers.append((fn, make_contextmanager(fn)))
        return Entry._load_from_dump_files(filename_readers, **kwargs)

    def update_data(self, readflo, data_version):
        readflo = _coerce_to_readflo(readflo)
        if int(self.current_version) == int(data_version):
            raise ValueError("cannot update with same version")
        if int(self.current_version) > int(data_version):
            raise ValueError("cannot update with older version")
        data = readflo.read()
        has_diff = data != self._incarnations[-1].data
        inc = DataIncarnation(data=data, data_version=data_version)
        self._incarnations.append(inc)
        self._versioninfo = self._versioninfo._replace(last_contained_version=data_version)
        if has_diff:
            self._versioninfo = self._versioninfo._replace(last_contained_version_with_diff=data_version)

    def _has_data(self):
        return self._versioninfo and (self._chain_length is not None)
    
    def _make_fileinfo(self):
        assert self._has_data()
        return filenames.FileInfo(
            key=self._key,
            first_version=self._versioninfo.first_contained_version,
            last_version=self._versioninfo.last_contained_version,
            depends_on_version=self._versioninfo.depends_on_external_version,
            dependency_chain_length=self._chain_length,
        )

    def _make_nameinfo(self):
        return filenames.compute_nameinfo(self._make_fileinfo())

    def _make_metadata_header(self):
        return _make_header(self._make_nameinfo(), self._versioninfo, self.key)

    def _generate_records(self):
        last = self._external_last_version
        prev = {}
        for inc in self._incarnations:
            if last and inc.data_version <= last.data_version:
                raise ValueError("incarnations are in inconsistent state (out of order)")
            h = inc.content_hash_digest
            rec = inc.as_record(baseline=last, previous_by_content=prev)
            yield rec
            last = inc
            prev[h] = inc

    def _write_named_json(self, opener):
        hdr = self._make_metadata_header()
        with opener(hdr["name"]) as binary_out:
            out = codecs.getwriter("utf-8")(binary_out)
            out.write("""{"datawatch":{"header":""")
            json.dump(hdr, out, indent="  ")
            out.write(""","content":[""")
            for i, rec in enumerate(self._generate_records()):
                if i > 0:
                    out.write(",")
                json.dump(rec, out, indent="  ")
            out.write("]}}\n")
    
    def _write_json(self, out):
        @contextlib.contextmanager
        def dummy_opener(name):
            yield out
        self._write_named_json(dummy_opener)

    def flush(self, dependency_chain_length_limit=10):
        if len(self._incarnations) < 2:
            return
        self._external_last_version = self._incarnations[-2]
        cur = self._incarnations[-1]
        has_diff = not cur.same_data_as(self._external_last_version)
        self._incarnations = [cur]
        self._chain_length = self._chain_length + 1
        self._versioninfo = self._versioninfo._replace(
            first_contained_version=cur.data_version,
            last_contained_version=cur.data_version,
            last_contained_version_with_diff=cur.data_version if has_diff else None,
            depends_on_external_version=self._external_last_version.data_version)
        if dependency_chain_length_limit is not None:
            if self._chain_length > dependency_chain_length_limit:
                self._external_last_version = None
                self._chain_length = 0
                self._versioninfo = self._versioninfo._replace(depends_on_external_version=None)

    def _find_incarnation(self, target):
        assert self._versioninfo.first_contained_version <= target <= self._versioninfo.last_contained_version
        # XXX index and/or binary search
        last = None
        for inc in self._incarnations:
            if target == inc.data_version:
                return inc
            if target < inc.data_version:
                assert last
                assert target >= last.data_version
                return last
            last = inc
        assert self._versioninfo.last_contained_version == last.data_version == target
        return last

    def read_data_bytes_at(self, data_version):
        with self.read_data_at(data_version) as f:
            return f.read()
    
    @contextlib.contextmanager
    def read_data_at(self, data_version):
        if data_version < self._versioninfo.first_known_version:
            raise ValueError("no data known at version {}; first known version is {}".format(data_version, self._versioninfo.first_known_version))
        if data_version < self._versioninfo.first_contained_version:
            raise ValueError("no data loaded at version {}; data prior to {} has been flushed".format(data_version, self._versioninfo.first_contained_version))
        if data_version > self.current_version:
            raise ValueError("no version known at {}: latest known version is {}".format(data_version, self.current_version))
        inc = self._find_incarnation(data_version)
        assert data_version >= inc.data_version
        flo = io.BytesIO(inc.data)
        try:
            yield flo
        finally:
            flo.close()

    def __iter__(self):
        for inc in self.loaded_versions():
            yield inc

    def incarnations(self):
        for inc in self._incarnations:
            yield inc

    def loaded_versions(self):
        return [inc.data_version for inc in self._incarnations]

    @property
    def keyhash(self):
        return self._keyhash

    @property
    def current_content_hash_digest(self):
        return self._incarnations[-1].content_hash_digest

    def get_oldest_data_age(self, current_version=None):
        current_version = current_version or self.current_version
        return int(current_version) - int(self._versioninfo.first_contained_version)
    
    @property
    def current_version(self):
        return self._versioninfo.last_contained_version

    @property
    def key(self):
        return self._key

    @property
    def info(self):
        return self._make_nameinfo()

    def compute_stats(self):
        total_size = 0
        buf = io.BytesIO()
        self._write_json(buf)
        serialized = buf.getvalue()
        for inc in self._incarnations:
            total_size += len(inc.data)
        compressed = zlib.compress(serialized)
        ratio = len(serialized) / total_size
        compressed_ratio = len(compressed) / total_size
        return {
            "serialized_json_size_bytes": len(serialized),
            "serialized_compressed_json_size_bytes": len(compressed),
            "number_of_incarnations": len(self._incarnations),
            "total_data_size_bytes": total_size,
            "ratio": ratio,
            "compressed_ratio": compressed_ratio,
        }

def _make_example():
    import io
    u = "https://example.com/foo"
    data = b"mycontent"
    ver = "123456789"
    new_entry = Entry.create_initial(u, data, ver)
    new_entry.update_data(io.BytesIO(b"newcontent"), "123546789")
    new_entry.update_data(io.BytesIO(b"morecontent"), "123746789")
    new_entry.update_data(io.BytesIO(repr([i for i in range(10000)]).encode("utf-8")), "123746889")
    new_entry.update_data(io.BytesIO(repr([(i if i != 42 else 43) for i in range(10000)]).encode("utf-8")), "123746900")
    new_entry.update_data(io.BytesIO(repr([(i if i != 42 else 44) for i in range(10000)]).encode("utf-8")), "123746910")
    new_entry.update_data(io.BytesIO(repr([i for i in range(10000)]).encode("utf-8")), "123800000")
    new_entry.update_data(io.BytesIO(repr([(i if i != 42 else 72) for i in range(10000)]).encode("utf-8")), "123986910")
    import string
    import random
    xs = "".join([random.choice(string.ascii_letters) for i in range(10000)])
    ys = "".join([random.choice(string.ascii_letters) for i in range(10000)])
    new_entry.update_data(io.BytesIO((xs+"a"+ys).encode("utf-8")), "124000002")
    new_entry.update_data(io.BytesIO((xs+"z"+ys).encode("utf-8")), "124000702")
    return new_entry

def read_streaming(store, key_filter=None, include_unchanged=False):
    assert key_filter or (key_filter is None)
    keyhashes = Collection(store).get_keyhash_names_from_storage()
    only_keys = only_keyhashes = None
    if key_filter is not None:
        only_keys = set(key_filter)
        only_keyhashes = set(methods.compute_key_hash(k)["digest"] for k in only_keys)
    for kh in keyhashes:
        if (key_filter is not None) and kh not in only_keyhashes:
            continue
        # TODO optimize or at least make actually streaming.
        # don't need to load the entire history at once.
        entry = Collection(store, full_history=True)[kh]
        if (key_filter is not None) and entry.key not in only_keys:
            continue
        last_data = None
        for inc in entry.incarnations():
            if inc.data == last_data and not include_unchanged:
                continue
            last_data = inc.data
            yield entry, inc

class Collection(object):
    def __init__(self, storage, flush_settings=None, full_history=False):
        self._storage = storage
        self._entries = {}
        self._keys = set()
        self._keyhashes = set()
        self._flush_settings = dict(flush_settings or {})
        self._full_history = full_history

    def _compute_keyhash(self, key):
        return methods.compute_key_hash(key)["digest"]

    def _try_get_entry_by_keyhash(self, keyhash):
        try:
            return self._entries[keyhash]
        except KeyError:
            pass
        # Attempt to load it.
        names = self._storage.list_filtered_chunks(keyhash_filter=[keyhash])
        if not names:
            return None
        if not self._full_history:
            entry = Entry.load_dumps(self._storage, names, only_from_last_checkpoint=True)
            entry.flush(dependency_chain_length_limit=0)
        else:
            entry = Entry.load_dumps(self._storage, names, full_history=True)
        self._entries[keyhash] = entry
        self._keys.add(entry.key)
        self._keyhashes.add(entry.info.keyhash)
        return entry

    def _try_get_entry_by_key(self, key):
        keyhash = self._compute_keyhash(key)
        entry = self._try_get_entry_by_keyhash(keyhash)
        if entry is None:
            return entry
        if entry.key != key:
            raise ValueError("hash collision ({} vs. {} both map to {}; bailing out)".format(entry.key, key, kh))
        return entry

    def _get_entry_by_key_and_update(self, key, data, data_version):
        kh = self._compute_keyhash(key)
        entry = self._try_get_entry_by_keyhash(kh)
        if entry is None:
            entry = Entry.create_initial(key, data, data_version)
            self._entries[kh] = entry
            self._keys.add(entry.key)
            self._keyhashes.add(entry.info.keyhash)
        else:
            entry.update_data(io.BytesIO(data), data_version)
        if entry.key != key:
            raise ValueError("hash collision ({} vs. {} both map to {}; bailing out)".format(entry.key, key, kh))
        return entry

    def update_data(self, key, data, data_version):
        readflo = _coerce_to_bytes(data)
        return self._get_entry_by_key_and_update(key, data, data_version)
    
    def entry_by_key(self, key):
        rv = self._try_get_entry_by_key(key)
        if rv is None:
            raise KeyError(key)
        return rv

    def __iter__(self):
        for kh in self._keyhashes:
            yield kh

    def __getitem__(self, keyhash):
        rv = self._try_get_entry_by_keyhash(keyhash)
        if rv is None:
            raise KeyError(keyhash)
        return rv

    def _determine_last_stored_version(self, keyhash, store):
        files = store.list_filtered_chunks(keyhash_filter=[keyhash])
        fnis = [filenames.decode_filename(fn) for fn in files]
        if not fnis:
            return None
        return max([fni.last_version for fni in fnis])

    def _write_to_storage_and_flush(self, entry, store):
        kh = entry.info.keyhash
        last_stored_version = self._determine_last_stored_version(kh, store)
        if last_stored_version is None:
            entry.write_dump(store)
            return True
        have_more_recent = int(entry.current_version) > int(last_stored_version)
        if not have_more_recent:
            return False
        entry.write_dump(store)
        return True

    def _sync_to_other(self, other_coll):
        did = False
        for kh in self:
            entry = self[kh]
            if self._write_to_storage_and_flush(entry, other_coll._storage):
                did = True
        return did

    def load_keyhash_from_storage(self, keyhash):
        if self._try_get_entry_by_keyhash(keyhash) is None:
            raise ValueError("keyhash data not found: {}".format(keyhash))

    def get_keyhash_names_from_storage(self):
        keyhashes = set()
        for chunk in self._storage.list_chunks():
            keyhashes.add(filenames.decode_filename(chunk).keyhash)
        keyhashes = list(keyhashes)
        keyhashes.sort()
        return keyhashes

    def load_all_from_storage(self):
        for kh in self.get_keyhash_names_from_storage():
            self.load_keyhash_from_storage(kh)

    def _summarize_to_specific(self, other_coll, khs):
        hist = Collection(self._storage, full_history=True)
        for kh in khs:
            hist._try_get_entry_by_keyhash(kh)
        hist._sync_to_other(other_coll)

    def summarize_to(self, other_coll):
        return self._summarize_to_specific(other_coll, list(self))
    
    def _sync_and_flush_single(self, kh):
        entry = self[kh]
        if self._write_to_storage_and_flush(entry, self._storage):
            did = True
        entry.flush(**self._flush_settings)
        self._last_flushed[kh] = time.time()

    def summarize_one_to(self, other_coll):
        kh = random.choice(list(self))
        return self._summarize_to_specific(other_coll, [kh])

    def sync_and_flush_one(self):
        khs = list(self)
        tried = set()
        while True:
            never_flushed = []
            items = []
            for kh in khs:
                if kh in tried:
                    continue
                if kh not in self._last_flushed:
                    never_flushed.append(kh)
                else:
                    items.append((self._last_flushed[kh], kh))
            if never_flushed:
                chosen = random.choice(never_flushed)
            else:
                if not items:
                    return False
                items.sort()
                _, chosen = items[0]
            tried.add(chosen)
            did = self._sync_and_flush_single(chosen)
            if did:
                return True
        return False

if __name__ == "__main__":
    import sys
    import time
    import requests
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"
    headers = {
        "User-Agent": user_agent,
    }
    target_url = sys.argv[1]
    target_dir = sys.argv[2]
    summary_dir = sys.argv[3]
    coll = Collection(storage.LocalFileStorage(target_dir))
    summarized = Collection(storage.LocalFileStorage(summary_dir))
    def fetch():
        return requests.get(target_url, headers=headers).content
    def now():
        return str(int(time.time()*1e9))
    last_summary = time.time()
    while True:
        data = fetch()
        t = now()
        coll.update_data(target_url, data, t)
        time.sleep(5)
        coll.sync_and_flush()
        if (time.time() - last_summary) > 60:
            coll.summarize_to(summarized)
            last_summary = time.time()
