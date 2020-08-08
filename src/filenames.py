import methods

import collections

FileInfo = collections.namedtuple("FileInfo", [
    "key",
    "first_version",
    "last_version",
    "depends_on_version",
    "dependency_chain_length",
])

FilenameEncodedInfo = collections.namedtuple("FilenameEncodedInfo", [
    "maybe_key",
    "last_version",
    "first_version",
    "depends_on_version",
    "dependency_chain_length",
    "keyhash",
    "encoded_key_prefix",
    "key_prefix",
    "key_length",
    "version_span",
    "version_shard",
])

_FILENAME_SUFFIX = ".datawatch.json"
_FILENAME_TMPL = (
    "{version_shard}/{keyhash}/{last_version}.{version_span}."
    "{externaldep_or_zero}.{chainlen}.{key_length}."
    "{encoded_key_prefix}{filename_suffix}"
)
_MAX_FILENAME_LENGTH = 768

def _encode_filename_from_filenameinfo(fni):
    externaldep_or_zero = fni.depends_on_version or 0
    first_version = str(int(fni.last_version) - int(fni.version_span))
    rv = _FILENAME_TMPL.format(
        version_shard=fni.version_shard,
        keyhash=fni.keyhash,
        first_version=first_version,
        last_version=fni.last_version,
        version_span=fni.version_span,
        externaldep_or_zero=externaldep_or_zero,
        chainlen=fni.dependency_chain_length,
        key_length=fni.key_length,
        encoded_key_prefix=fni.encoded_key_prefix,
        filename_suffix=_FILENAME_SUFFIX,
    )
    assert len(rv) <= _MAX_FILENAME_LENGTH
    return rv

def _decode_filename_to_filenameinfo(filename):
    if filename.count("/") != 2:
        raise ValueError("invalid number of slashes in filename")
    version_shard, keyhash, rest = filename.split("/")
    if not rest.endswith(_FILENAME_SUFFIX):
        raise ValueError("filename does not end with " + _FILENAME_SUFFIX)
    rest = rest[:len(rest)-len(_FILENAME_SUFFIX)]
    if rest.count(".") != 5:
        raise ValueError("invalid number of dots in filename")
    last_version, version_span, externaldep_or_zero, chainlen, key_length, encoded_key_prefix = rest.split(".")
    first_version = str(int(last_version) - int(version_span))
    key_prefix = methods.decode_key_prefix(encoded_key_prefix)
    return FilenameEncodedInfo(
        maybe_key=key_prefix if int(key_length) == len(key_prefix) else None,
        keyhash=keyhash,
        key_length=int(key_length),
        key_prefix=key_prefix,
        encoded_key_prefix=encoded_key_prefix,
        version_span=version_span,
        first_version=first_version,
        last_version=last_version,
        depends_on_version=None if int(externaldep_or_zero) == 0 else externaldep_or_zero,
        dependency_chain_length=int(chainlen),
        version_shard=version_shard,
    )

def decode_filename(filename):
    return _decode_filename_to_filenameinfo(filename)

def compute_nameinfo(fileinfo):
    if not fileinfo.key:
        raise ValueError("no key provided")
    key = fileinfo.key
    ver0, ver1 = fileinfo.first_version, fileinfo.last_version
    verdep, chainlen = fileinfo.depends_on_version, fileinfo.dependency_chain_length
    key_length = len(key)
    keyhash = methods.compute_key_hash(key)["digest"]
    ver0 = int(ver0)
    ver1 = int(ver1)
    if ver1 < ver0:
        raise ValueError("last version cannot be smaller than first version")
    if verdep:
        if int(verdep) >= ver0:
            raise ValueError("dependent version must be smaller than first version")
    span = ver1 - ver0
    ver = ver1
    version_shard = methods.compute_version_shard(str(ver))
    if verdep:
        if chainlen <= 0:
            raise ValueError("invalid dependency chain length for dependent file")
    else:
        if chainlen != 0:
            raise ValueError("invalid dependency chain length for independent file")
    keyprefix, keyprefix_len = methods.encode_key_prefix(key)
    encoded_key = key if (keyprefix_len == len(key)) else None
    return FilenameEncodedInfo(
        maybe_key=encoded_key,
        keyhash=keyhash,
        key_length=len(key),
        key_prefix=key[:keyprefix_len],
        encoded_key_prefix=keyprefix,
        version_span=str(span),
        last_version=str(ver1),
        first_version=str(ver0),
        depends_on_version=verdep,
        dependency_chain_length=chainlen,
        version_shard=version_shard,
    )

def encode_filename(fileinfo):
    filenameinfo = compute_nameinfo(fileinfo)
    return _encode_filename_from_filenameinfo(filenameinfo)

def encode_filename_from_nameinfo(filenameinfo):
    return _encode_filename_from_filenameinfo(filenameinfo)
