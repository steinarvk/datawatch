from .filenames import *

import pytest

def test_encode_and_decode_filename():
    infos = [
        FileInfo(
            key=u"my simple key",
            first_version="123456789",
            last_version="123456789",
            depends_on_version=None,
            dependency_chain_length=0,
        ),
        FileInfo(
            key=u"hello/world/my.key.with ÆØÅ \"and \'",
            first_version="123456789",
            last_version="123758400",
            depends_on_version="123444444",
            dependency_chain_length=1,
        ),
        FileInfo(
            key=u"my simple key",
            first_version="123456789",
            last_version="123456789",
            depends_on_version="5002",
            dependency_chain_length=10,
        ),
        FileInfo(
            key="But, in a larger sense, we can not dedicate -- we can not consecrate -- we can not hallow -- this ground. The brave men, living and dead, who struggled here, have consecrated it, far above our poor power to add or detract. The world will little note, nor long remember what we say here, but it can never forget what they did here. It is for us the living, rather, to be dedicated here to the unfinished work which they who fought here have thus far so nobly advanced. It is rather for us to be here dedicated to the great task remaining before us -- that from these honored dead we take increased devotion to that cause for which they gave the last full measure of devotion -- that we here highly resolve that these dead shall not have died in vain -- that this nation, under God, shall have a new birth of freedom -- and that government of the people, by the people, for the people, shall not perish from the earth.",
            first_version="123456789",
            last_version="123456789",
            depends_on_version=None,
            dependency_chain_length=0,
        ),
        FileInfo(
            key=u"my simple key",
            first_version="123450000",
            last_version="123456789",
            depends_on_version="5002",
            dependency_chain_length=10,
        ),
    ]
    saw_full, saw_short = False, False
    for info in infos:
        filename = encode_filename(info)
        decoded_info = decode_filename(filename)
        assert decoded_info.first_version == info.first_version
        assert decoded_info.last_version == info.last_version
        assert decoded_info.depends_on_version == info.depends_on_version
        assert decoded_info.dependency_chain_length == info.dependency_chain_length
        assert decoded_info.key_length == len(info.key)
        assert info.key.startswith(decoded_info.key_prefix)
        assert decoded_info.encoded_key_prefix
        assert info.last_version.startswith(decoded_info.version_shard.strip("0"))
        assert info.last_version >= decoded_info.version_shard
        assert len(info.last_version) == len(decoded_info.version_shard)
        if decoded_info.maybe_key is None:
            assert len(info.key) == decoded_info.key_length > len(decoded_info.key_prefix) > 0
            assert decoded_info.maybe_key != decoded_info.key_prefix
            saw_short = True
        else:
            assert decoded_info.maybe_key == decoded_info.key_prefix
            saw_full = True
        assert encode_filename_from_nameinfo(decoded_info) == filename
    assert saw_short
    assert saw_full

def test_encode_and_decode_filename_failure():
    bad_infos = [
        FileInfo(
            key=u"my simple key",
            first_version="123456790",
            last_version="123456789",
            depends_on_version=None,
            dependency_chain_length=0,
        ),
        FileInfo(
            key=u"hello/world/my.key.with ÆØÅ \"and \'",
            first_version="123456789",
            last_version="123758400",
            depends_on_version="123444444",
            dependency_chain_length=0,
        ),
        FileInfo(
            key=u"my simple key",
            first_version="123456789",
            last_version="123456789",
            depends_on_version=None,
            dependency_chain_length=-1,
        ),
        FileInfo(
            key="But, in a larger sense, we can not dedicate -- we can not consecrate -- we can not hallow -- this ground. The brave men, living and dead, who struggled here, have consecrated it, far above our poor power to add or detract. The world will little note, nor long remember what we say here, but it can never forget what they did here. It is for us the living, rather, to be dedicated here to the unfinished work which they who fought here have thus far so nobly advanced. It is rather for us to be here dedicated to the great task remaining before us -- that from these honored dead we take increased devotion to that cause for which they gave the last full measure of devotion -- that we here highly resolve that these dead shall not have died in vain -- that this nation, under God, shall have a new birth of freedom -- and that government of the people, by the people, for the people, shall not perish from the earth.",
            first_version="123456789a",
            last_version="123456789a",
            depends_on_version=None,
            dependency_chain_length=0,
        ),
    ]
    for info in bad_infos:
        pytest.raises(Exception, lambda: encode_filename(info))
