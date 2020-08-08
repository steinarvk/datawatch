import zlib
import hashlib
import bsdiff4
import base64
import functools

class _VersionSharder(object):
    def __init__(self, digits=5):
        self._digits = digits

    @property
    def version_sharding_method(self):
        return "{}digits-zero".format(self._digits)

    def get_version_shard(self, ver):
        if not ver.isdigit():
            raise ValueError("version must be decimal string")
        prefix = ver[:self._digits]
        shard = prefix + "0" * len(ver[self._digits:])
        assert len(shard) == len(ver)
        return shard

class _KeyEncoding(object):
    @property
    def key_encoding_method(self):
        return "unpad . base64.urlsafe_b64encode . zlib.compress"

    def encode(self, data):
        msg = data.encode("utf-8")
        compressed = zlib.compress(msg)
        assert zlib.decompress(compressed) == data.encode("utf-8")
        b64 = base64.urlsafe_b64encode(compressed).decode("ascii").strip("=\n")
        return b64

    def decode(self, encoded):
        pad = "=" * (4 - len(encoded) % 4)
        padded = encoded + pad
        compressed = base64.urlsafe_b64decode(padded)
        uncompressed = zlib.decompress(compressed)
        return uncompressed.decode("utf-8")

class _Hasher(object):
    @property
    def hash_method(self):
        return "sha256-hex"

    def hash_bytes(self, data):
        m = hashlib.sha256()
        m.update(data)
        return {
          "method": self.hash_method,
          "digest": m.hexdigest(),
        }

class _Differ(object):
    @property
    def diff_method(self):
        return "zlib.compress . bsdiff4.diff"

    def diff(self, a, b):
        return zlib.compress(bsdiff4.diff(a, b))

    def patch(self, a, patch):
        p = zlib.decompress(patch)
        return bsdiff4.patch(a, p)

DEFAULT_DIFFER = _Differ()
DEFAULT_HASHER = _Hasher()
DEFAULT_KEY_ENCODING = _KeyEncoding()
DEFAULT_SHARDER = _VersionSharder()

ACTIVE_METHODS = {
    "diff": DEFAULT_DIFFER.diff_method,
    "key_encoding": DEFAULT_KEY_ENCODING.key_encoding_method,
    "version_sharding": DEFAULT_SHARDER.version_sharding_method,
    "hash": DEFAULT_HASHER.hash_method,
}

ENCODED_KEY_LENGTH_LIMIT = 256

_CACHE_SIZE = 1024

def compute_content_hash(data):
    return DEFAULT_HASHER.hash_bytes(data)

def compute_diff(a, b):
    return DEFAULT_DIFFER.diff(a, b)

def apply_patch(a, atob):
    return DEFAULT_DIFFER.patch(a, atob)

@functools.lru_cache(maxsize=_CACHE_SIZE)
def compute_version_shard(ver):
    return DEFAULT_SHARDER.get_version_shard(ver)

@functools.lru_cache(maxsize=_CACHE_SIZE)
def compute_key_hash(key):
    return DEFAULT_HASHER.hash_bytes(key.encode("utf-8"))

@functools.lru_cache(maxsize=_CACHE_SIZE)
def decode_key_prefix(encoded):
    return DEFAULT_KEY_ENCODING.decode(encoded)

@functools.lru_cache(maxsize=_CACHE_SIZE)
def encode_key_prefix(key):
    enc = DEFAULT_KEY_ENCODING.encode
    simple = enc(key)
    if len(simple) <= ENCODED_KEY_LENGTH_LIMIT:
        return simple, len(key)
    low = 0
    encoded = enc(key[:low])
    assert len(encoded) <= ENCODED_KEY_LENGTH_LIMIT
    high = len(key)
    candidate = encoded
    while high > low:
        mid = (high + low) // 2
        if mid == low:
            break
        prefix = key[:mid]
        assert len(prefix) == mid
        encoded = enc(prefix)
        if len(encoded) <= ENCODED_KEY_LENGTH_LIMIT:
            candidate = encoded
            low = mid
        else:
            high = mid
        assert high >= low
    assert low == (high-1) == mid
    return candidate, mid
