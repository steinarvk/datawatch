import scheduling
import requests
import bs4
import uritools
import collections

class FetcherLoop(object):
    def __init__(self, on_fetched, user_agent, target_link_filter, fetching_ratelimit=0.2, discovery_delay=300, fetch_delay=60, verbose=False, exponential_backoff=None):
        self._loop = scheduling.SchedulingLoop(global_ratelimit=fetching_ratelimit, verbose=verbose)
        self._targets_by_root = collections.defaultdict(set)
        self._discovery_delay = discovery_delay
        self._fetch_delay = fetch_delay
        session = requests.session()
        headers = {"User-Agent": user_agent}
        timeout = 60
        def geturl(url, allow_failure=False):
            resp = session.get(url, headers=headers, timeout=timeout)
            # TODO: on receiving Last-Modified, cache responses and send If-Modified-Since in headers
            print("getting url", url, "got it:", resp)
            if not allow_failure:
                resp.raise_for_status()
            return resp
        self._initial_discovery_delay = 1.0
        self._on_fetched = on_fetched
        self._geturl = geturl
        self._target_link_filter = target_link_filter
        self._exponential_backoff = exponential_backoff

    def _has_target(self, target):
        for k, targetset in self._targets_by_root.items():
            if target in targetset:
                return True
        return False

    def _discovery_extract_links(self, data, content_type, discovery_url):
        soup = bs4.BeautifulSoup(data, features="html.parser")
        links = [el.attrs["href"] for el in soup.find_all("a") if "href" in el.attrs]
        links = [uritools.urijoin(discovery_url, link) for link in links]
        rv = list(set([link for link in links if self._target_link_filter(link)]))
        return rv

    def _add_target(self, url):
        delay = scheduling.as_delay(self._fetch_delay)
        def should_reschedule():
            return self._has_target(url)
        last_content = [None]
        consecutive_nochange = [0]
        def compute_reschedule_delay():
            n = consecutive_nochange[0]
            if self._exponential_backoff is None or n == 0:
                return delay()
            assert 10 > self._exponential_backoff > 1
            raw_delay = delay()
            multiplier = self._exponential_backoff ** n
            return multiplier * delay()
        def run_fetch(task):
            url = task.payload
            resp = self._geturl(url, allow_failure=True)
            content = resp.content
            changed = last_content[0] != content
            if not changed:
                consecutive_nochange[0] += 1
            else:
                consecutive_nochange[0] = 0
            print(url, "has changed?", changed, "nochange counter now at", consecutive_nochange[0])
            last_content[0] = content
            self._on_fetched(url, resp, content)
        print("adding new target for", url)
        self._loop.schedule_task(
            callback=run_fetch,
            payload=url,
            delay=self._fetch_delay,
            reschedule_if=should_reschedule,
            reschedule_delay=compute_reschedule_delay,
        )

    def _run_discovery(self, task):
        discovery_root_url = task.payload
        resp = self._geturl(discovery_root_url)
        ctype = resp.headers["Content-Type"]
        discovered = self._discovery_extract_links(resp.content, resp.headers["Content-Type"], discovery_root_url)
        newly = [x for x in discovered if not self._has_target(x)]
        self._targets_by_root[discovery_root_url] = set(discovered)
        for new_target_url in newly:
            self._add_target(new_target_url)

    def schedule_nonfetching_task(self, **kwargs):
        self._loop.schedule_task(**kwargs, apply_global_ratelimit=False)
    
    def add_discovery_root(self, url):
        self._loop.schedule_task(
            callback=self._run_discovery,
            payload=url,
            delay=self._initial_discovery_delay,
            reschedule=True,
            reschedule_delay=self._discovery_delay,
        )
    
    def run_loop(self):
        self._loop.run_loop()

if __name__ == "__main__":
    user_agent = "Fetcherbot"
    target_link_filter = lambda url: url.startswith("https://docs.python.org/3/library/") and url.endswith(".html") and "cookiejar" in url
    def on_fetched(url, resp, content):
        print("response from", url, "was", resp, "with", len(content), "bytes of content")
        print("headers were", resp.headers)
    def do_something_else(task):
        print("hello there")
    loop = FetcherLoop(on_fetched=on_fetched, user_agent=user_agent, target_link_filter=target_link_filter, fetching_ratelimit=0.2, fetch_delay=10, verbose=True)
    loop.schedule_nonfetching_task(callback=do_something_else, delay=10.0)
    loop.add_discovery_root("https://docs.python.org/3/library/index.html")
    loop.run_loop()
