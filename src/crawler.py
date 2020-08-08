#!/usr/bin/env python
# encoding: utf-8

import click

import fetcher
import time
import re
import hashlib
import datadiff
import storage

@click.command()
@click.option("--root", multiple=True, help="Roots for target discovery.")
@click.option("--target_regex", multiple=True,
              help="Regex for filtering target URLs (match-any).")
@click.option("--user_agent", help="User agent to use for fetching.")
@click.option("--target_fetch_delay", default=60,
              help="Desired fetch delay for each target.")
@click.option("--rediscovery_delay", default=300,
              help="Desired fetch delay for each discovery root.")
@click.option("--fetching_rate_limit", default=0.2,
              help="Minimum delay between end of a fetch and start of next.")
@click.option("--checkpoint_output_dir",
              help="Output directory for checkpoints.")
@click.option("--summary_output_dir",
              help="Output directory for summaries.")
@click.option("--summary_delay", default=3600,
              help="Desired delay between summaries.")
@click.option("--checkpoint_delay", default=30,
              help="Desired delay between checkpoint attempts.")
@click.option("--exponential_backoff", default=None,
              help="Increase time to next fetch for resources that don't change much.")
def main(root, target_regex, user_agent, fetching_rate_limit, target_fetch_delay, rediscovery_delay, checkpoint_output_dir, summary_output_dir, summary_delay, checkpoint_delay, exponential_backoff):
    assert root
    assert target_regex
    assert user_agent
    assert checkpoint_output_dir
    compiled = [re.compile(x) for x in target_regex]
    def target_link_filter(url):
        for x in compiled:
            if x.match(url):
                return True
        return False
    coll = datadiff.Collection(storage.LocalFileStorage(checkpoint_output_dir))
    def now():
        return str(int(time.time()*1e9))
    def on_fetched(target_url, resp, content):
        coll.update_data(target_url, content, now())
    def sync_to_checkpoints(task):
        coll.sync_and_flush()
    mainloop = fetcher.FetcherLoop(
        on_fetched=on_fetched,
        user_agent=user_agent,
        target_link_filter=target_link_filter,
        fetching_ratelimit=fetching_rate_limit,
        discovery_delay=rediscovery_delay,
        fetch_delay=target_fetch_delay,
        exponential_backoff=exponential_backoff,
        verbose=True,
    )
    if summary_output_dir:
        summary_coll = datadiff.Collection(storage.LocalFileStorage(summary_output_dir))
        def do_summaries(task):
            coll.summarize_to(summary_coll)
        mainloop.schedule_nonfetching_task(callback=do_summaries, delay=summary_delay, reschedule=True)
    mainloop.schedule_nonfetching_task(callback=sync_to_checkpoints, delay=checkpoint_delay, reschedule=True)
    for oneroot in root:
        mainloop.add_discovery_root(oneroot)
    mainloop.run_loop()

if __name__ == "__main__":
    main()
