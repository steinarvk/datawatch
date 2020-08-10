#!/usr/bin/env python
# encoding: utf-8

import click
import io
import sys

import yaml
import storage
import datadiff
import hashlib
import methods

# TODO pattern to factor out:
#   option to choose something from named dictionary
# TODO pattern to factor out:
#   input/output file
#   (flags: filename defaulting to -, allow overwrite, binary/text mode)

@click.command()
@click.option("--data-dir",
              help="Input directory containing datawatch data.")
@click.option("--include-unchanged/--no-include-unchanged",
              default=False, show_default=True, type=bool,
              help="Perform reduction even when nothing has changed from the previous version.")
@click.option("--omit-data/--no-omit-data",
              default=False, show_default=True, type=bool,
              help="Omit the actual data from the output.")
@click.option("--extra-info/--no-extra-info",
              default=False, show_default=True, type=bool,
              help="Add some extra descriptive metadata in the output.")
@click.option("--select-key", multiple=True,
              help="Select only a specific set of keys.")
@click.option("--value-type", default="auto", show_default=True,
              help="Choose kind of value to output.")
def main(data_dir, include_unchanged, omit_data, extra_info, select_key, value_type):
    valuedecoders = {
        "auto": lambda rev: rev.get_data_as_bytes_or_unicode(),
        "raw": lambda rev: rev.data,
        "string": lambda rev: rev.get_data_as_unicode(),
    }
    try:
        valuedecoder = valuedecoders[value_type]
    except KeyError:
        raise ValueError("unknown or unhandled --value_type: {} (options: {})".format(repr(value_type), repr(list(valuedecoders))))
    stream = datadiff.read_streaming(
        store=storage.LocalFileStorage(data_dir),
        key_filter=select_key or None,
        include_unchanged=include_unchanged)
    for entry, revision in stream:
        record = {
            "key": entry.key,
            "data_version": revision.data_version,
        }
        if extra_info:
            record["info"] = {
                "keyhash": entry.keyhash,
                "data_length": len(revision.data),
                "data_hash": revision.content_hash_digest,
            }
        if not omit_data:
            record["value"] = valuedecoder(revision)
        yaml.safe_dump(record, explicit_start=True, explicit_end=True, stream=sys.stdout)

if __name__ == "__main__":
    main()
