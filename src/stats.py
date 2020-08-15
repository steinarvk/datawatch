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

@click.command()
@click.option("--data-dir",
              help="Input directory containing datawatch data.")
@click.option("--select-key", multiple=True,
              help="Select only a specific set of keys.")
def main(data_dir, select_key):
    stream = datadiff.read_streaming(
        store=storage.LocalFileStorage(data_dir),
        key_filter=select_key or None,
        include_unchanged=True)
    last_entry = None
    last_revision = None
    class C(object): pass
    ctx = C()
    def reset():
        ctx.num_revisions = 0
        ctx.num_revisions_with_diff = 0
        ctx.total_bytes = 0
        ctx.total_bytes_with_diff = 0
    def flush(entry):
        print("\t".join(map(str, (ctx.num_revisions, ctx.num_revisions_with_diff, ctx.total_bytes, ctx.total_bytes_with_diff, entry.keyhash, entry.key))))
    reset()
    for entry, revision in stream:
        if entry != last_entry:
            if last_entry:
                flush(last_entry)
            reset()
        diff = not revision.same_data_as(last_revision)
        ctx.num_revisions += 1
        ctx.total_bytes += revision.content_length
        if diff:
            ctx.num_revisions_with_diff += 1
            ctx.total_bytes_with_diff += revision.content_length
        last_revision = revision
        last_entry = entry
    if entry:
        flush(entry)

if __name__ == "__main__":
    main()
