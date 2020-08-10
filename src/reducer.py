#!/usr/bin/env python
# encoding: utf-8

import click
import io
import sys
import contextlib

import subprocess
import storage
import datadiff


@contextlib.contextmanager
def output_file(filename, text_mode=False, allow_overwrite=False):
    if filename == "-":
        yield sys.stdout
        return
    mode = "wb" if allow_overwrite else "xb"
    with open(filename, mode) as f:
        yield f

@click.command()
@click.option("--script",
              help="Script binary to call on each version.")
@click.option("--data-dir",
              help="Input directory containing datawatch data.")
@click.option("--include-unchanged/--no-include-unchanged",
              default=False, show_default=True, type=bool,
              help="Perform reduction even when nothing has changed from the previous version.")
@click.option("--allow-overwrite/--no-allow-overwrite",
              default=False, show_default=True, type=bool,
              help="Allow overwriting the output file.")
@click.option("--output", default="-", show_default=True, help="Output file.")
@click.option("--select-key", multiple=True,
              help="Select only a specific set of keys.")
def main(script, output, allow_overwrite, data_dir, include_unchanged, select_key):
    with output_file(output, allow_overwrite=allow_overwrite) as out:
        stream = datadiff.read_streaming(
            store=storage.LocalFileStorage(data_dir),
            key_filter=select_key or None,
            include_unchanged=include_unchanged)
        for entry, revision in stream:
            subprocess.run(
                [script, entry.key, revision.data_version],
                input = revision.data,
                stdout = out,
            ).check_returncode()

if __name__ == "__main__":
    main()
