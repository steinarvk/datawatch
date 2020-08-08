from .methods import *

def test_differ():
    a = repr(list(range(1000))).encode("utf-8")
    b = repr(list(range(1005))).encode("utf-8")
    diff = DEFAULT_DIFFER.diff(a, b)
    assert 0 < len(diff) < len(a) < len(b)
    new_b = DEFAULT_DIFFER.patch(a, diff)
    assert b == new_b

def test_hasher():
    hashed = DEFAULT_HASHER.hash_bytes(b"")
    assert hashed["method"] == "sha256-hex"
    assert hashed["digest"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

def test_compute_key_hash():
    hashed = compute_key_hash("")
    assert hashed["method"] == "sha256-hex"
    assert hashed["digest"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

def test_compute_version_shard():
    ver = "123456789123456789"
    ver2 = "123466789123456789"
    ver_shard = compute_version_shard(ver)
    ver_shard2 = compute_version_shard(ver2)
    assert ver.startswith(ver_shard.strip("0"))
    assert ver_shard < ver < ver_shard2 < ver2
    assert ver_shard == "123450000000000000"

def test_key_encoding():
    u = "https://www.example.com/foo/bar/baz"
    encoded = DEFAULT_KEY_ENCODING.encode(u)
    decoded = DEFAULT_KEY_ENCODING.decode(encoded)
    assert decoded == u

def test_sharder():
    ver = "123456789123456789"
    ver2 = "123466789123456789"
    ver_shard = DEFAULT_SHARDER.get_version_shard(ver)
    ver_shard2 = DEFAULT_SHARDER.get_version_shard(ver2)
    assert ver.startswith(ver_shard.strip("0"))
    assert ver_shard < ver < ver_shard2 < ver2
    assert ver_shard == "123450000000000000"

def test_key_prefix_encoding():
    import string
    import random
    got_full, got_short = False, False
    for i in range(1, 400, 10):
        s = "".join([random.choice(string.ascii_lowercase) for i in range(i)])
        assert len(s) == i
        encoded_prefix, n = encode_key_prefix(s)
        decoded_prefix = decode_key_prefix(encoded_prefix)
        print(s, encoded_prefix)
        assert decoded_prefix
        assert s.startswith(decoded_prefix)
        if i <= 200:
            assert n == len(s)
        if n == len(s):
            got_full = True
        else:
            got_short = True
        assert s[:n] == decoded_prefix
    assert got_full
    assert got_short
