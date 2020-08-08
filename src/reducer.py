#!/usr/bin/env python
# encoding: utf-8

import click
import io
import sys

import subprocess
import storage
import datadiff

@click.command()
@click.option("--script",
              help="Script binary to call on each version.")
@click.option("--data_dir",
              help="Input directory containing datawatch data.")
@click.option("--include_unchanged", default=False, show_default=True,
              help="Perform reduction even when nothing has changed from the previous version.")
@click.option("--allow_overwrite", default=False, show_default=True,
              help="Allow overwriting the output file.")
@click.option("--output", default="-", show_default=True, help="Output file.")
def main(script, data_dir, output, include_unchanged, allow_overwrite):
    store = storage.LocalFileStorage(data_dir)
    keyhashes = datadiff.Collection(store).get_keyhash_names_from_storage()
    def core(out):
        for kh in keyhashes:
            entry = datadiff.Collection(store, full_history=True)[kh]
            last_data = None
            for data_version in entry.loaded_versions():
                data = entry.read_data_bytes_at(data_version)
                if data == last_data and not include_unchanged:
                    continue
                last_data = data
                subprocess.run(
                    [script, entry.key, data_version],
                    input = data,
                    stdout = out,
                ).check_returncode()
    if output == "-":
        core(sys.stdout)
    else:
        with open(output_file, "wb" if allow_overwrite else "xb") as out:
            core(out)

if __name__ == "__main__":
    main()
