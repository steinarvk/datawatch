from .storage import *

def test_inmemory_storage():
    store = InMemoryStorage()
    assert list(store.list_chunks()) == []
    with store.write_chunk("foo/bar/baz") as f:
        f.write(b"hello")
        f.write(b"world")
    assert list(store.list_chunks()) == ["foo/bar/baz"]
    with store.read_chunk("foo/bar/baz") as f:
        assert f.read() == b"helloworld"
    with store.write_chunk("foo/bar/baz") as f:
        f.write(b"hello")
        f.write(b"world")
    assert list(store.list_filtered_chunks(version_shard_filter="foo")) == ["foo/bar/baz"]
    with store.write_chunk("foo/bar/quux") as f:
        pass
    assert list(store.list_chunks()) == ["foo/bar/baz", "foo/bar/quux"]
    with store.read_chunk("foo/bar/quux") as f:
        assert f.read() == b""
    with store.read_chunk("foo/bar/baz") as f:
        assert f.read() == b"helloworld"
    with store.write_chunk("foo/bar/baz") as f:
        f.write(b"overwrite")
    with store.read_chunk("foo/bar/baz") as f:
        assert f.read() == b"overwrite"
