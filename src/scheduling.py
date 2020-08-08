import random
import heapq
import time
import sys
import dataclasses
import collections

from typing import Any

def fuzzed_delay_generator(mean, fuzz_ratio=0.5, sigmas=3):
    if not (0 <= fuzz_ratio <= 1):
        raise ValueError("fuzzing ratio out of range: {}".format(fuzz_ratio))
    max_fuzz = mean * fuzz_ratio
    sigma = max_fuzz / sigmas
    def f():
        fuzz = None
        while fuzz is None or abs(fuzz) > max_fuzz:
            fuzz = random.normalvariate(0, sigma)
        return mean + fuzz
    return f

def as_delay(delay):
    if isinstance(delay, (int, float)):
        delay = fuzzed_delay_generator(delay)
    return delay

def always_reschedule():
    return True

def never_reschedule():
    return False

@dataclasses.dataclass(order=True)
class Task:
    trigger_time: float
    callback: callable
    name: str
    payload: Any = None
    apply_global_ratelimit: bool = True
    reschedule_if: callable = never_reschedule
    reschedule_delay: callable = None

DEFAULT_GLOBAL_RATELIMIT = fuzzed_delay_generator(0.2)

_VERY_SHORT_SLEEP = 0.0001

class SchedulingLoop(object):
    def __init__(self, global_ratelimit=None, clock=None, sleep=None, verbose=False):
        self._tasks = []
        self._clock = clock or time.time
        self._sleep = sleep or time.sleep
        self._verbose = verbose
        self._global_ratelimit_delay = as_delay(global_ratelimit or DEFAULT_GLOBAL_RATELIMIT)
        self._global_ratelimit_last_end = None
        self._global_ratelimit_next_delay = None

    def _wait_for_global_ratelimit(self):
        if not self._global_ratelimit_last_end:
            return
        if self._global_ratelimit_next_delay is None:
            self._global_ratelimit_next_delay = self._global_ratelimit_delay()
        since = self._clock() - self._global_ratelimit_last_end
        shortfall = self._global_ratelimit_next_delay - since
        if shortfall > 0:
            self.log("waiting", shortfall, "for global rate limit")
            self._sleep(shortfall)
        self._global_ratelimit_next_delay = None
    
    def add_task(self, task):
        self.log("scheduling task", task.name, "for", task.trigger_time)
        heapq.heappush(self._tasks, task)

    def schedule_task(self, delay, **kwargs):
        delay = as_delay(delay)
        trigger_time = self._clock() + delay() 
        kwargs = dict(kwargs)
        if "reschedule_delay" in kwargs:
            kwargs["reschedule_delay"] = as_delay(kwargs["reschedule_delay"])
        if kwargs.get("reschedule") == True:
            del kwargs["reschedule"]
            kwargs["reschedule_if"] = always_reschedule
        if "reschedule_if" in kwargs:
            if "reschedule_delay" not in kwargs:
                kwargs["reschedule_delay"] = delay
        if "name" not in kwargs:
            kwargs["name"] = kwargs["callback"].__name__
        new_task = Task(trigger_time=trigger_time, **kwargs)
        self.add_task(new_task)
        return new_task

    def log(self, *args, **kwargs):
        if self._verbose:
            kwargs = dict(kwargs, file=sys.stderr)
            print(*args, **kwargs)

    def run_once(self):
        now = self._clock()
        task = self._tasks[0]
        if task.trigger_time > now:
            sleeptime = min(_VERY_SHORT_SLEEP, task.trigger_time - now)
            # self.log("task", task.name, "not ready yet at", now, "intended for", task.trigger_time, "sleeping", sleeptime)
            self._sleep(sleeptime)
            return False
        heapq.heappop(self._tasks)
        if task.apply_global_ratelimit:
            self._wait_for_global_ratelimit()
        self.log("running task", task.name, "at", now, "intended for", task.trigger_time, "delay", now - task.trigger_time)
        t0 = self._clock()
        try:
            task.callback(task)
        finally:
            t1 = self._clock()
            if task.apply_global_ratelimit:
                self._global_ratelimit_last_end = t1
            self.log("ran", task.name, "taking", t1-t0)
            if task.reschedule_if and task.reschedule_if():
                delay = task.reschedule_delay()
                self.log("rescheduling", task.name, "for", delay, "from previous start, in", (now + delay) - t1)
                new_task = dataclasses.replace(task, trigger_time=now + delay)
                self.add_task(new_task)
        return True

    def run_loop(self):
        while True:
            self.run_once()

if __name__ == "__main__":
    loop = SchedulingLoop(verbose=False, global_ratelimit=fuzzed_delay_generator(0.2))
    t0 = time.time()
    says = collections.Counter()
    def say(task):
        says[task.payload] += 1
        elapsed = time.time() - t0
        print("saying", task.payload, "mean time between:", elapsed / says[task.payload])
        if task.payload == "boo!":
            loop.schedule_task(callback=say, delay=fuzzed_delay_generator(0.2), payload="eek!")
    for i in range(3):
        loop.schedule_task(callback=say, delay=fuzzed_delay_generator(0.4), payload="hello{}".format(i+1), reschedule=True, apply_global_ratelimit=False)
    loop.schedule_task(callback=say, delay=fuzzed_delay_generator(1.2), payload="foo", reschedule=True)
    loop.schedule_task(callback=say, delay=fuzzed_delay_generator(5), payload="boo!", reschedule=True)
    loop.run_loop()
