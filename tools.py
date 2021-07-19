import random
import string
import numpy as np
import time
import heapq
from collections import deque


def generate_random_string(start=5, end=15):
    random_string = ''.join(random.choice(string.ascii_lowercase) for _ in range(np.random.randint(start, end)))
    return random_string


# Decorator that reports to a log file the execution time
def log_time(func):
    def wrapper(self, *args, **kwargs):
        start = time.perf_counter()
        func(self, *args, **kwargs)
        finish = time.perf_counter()
        run_time = finish - start
        self.logger.info('=' * 30 + f'{func.__name__} Run Time: {run_time} seconds ' + '=' * 30)
    return wrapper


def k_merge(*lists):
    queues = [queue for queue in map(deque, lists)]

    heap = []
    for i, lst in enumerate(queues):
        heap.append((lst.popleft(), i))

    heapq.heapify(heap)

    result = []
    while heap:
        value, index = heapq.heappop(heap)
        result.append(value)

        if queues[index]:
            heapq.heappush(heap, (queues[index].popleft(), index))

    return result
