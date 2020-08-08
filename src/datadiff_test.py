from .datadiff import *

import datadiff

import io
import json

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
    return new_entry

def test_serialize_to_json():
    ex = _make_example()
    f = io.BytesIO()
    ex._write_json(f)
    jsondata = f.getvalue().decode("utf-8")
    parsed = json.loads(jsondata)

def test_read_at_specific_points():
    ex = _make_example()
    with ex.read_data_at("123746789") as f:
        assert f.read() == b"morecontent"
    with ex.read_data_at("123746788") as f:
        assert f.read() == b"newcontent"

def test_get_versions():
    ex = _make_example()
    vers = ex.loaded_versions()
    assert len(vers) == 8
    for cv in vers:
        with ex.read_data_at(cv) as f:
            f.read()

def test_coercing_to_bytes():
    assert datadiff._coerce_to_bytes(io.BytesIO(b"hello")) == b"hello"
    assert datadiff._coerce_to_bytes(b"hello") == b"hello"
    assert datadiff._coerce_to_bytes("hello") == b"hello"
