import asyncio
import time
import uuid

import numpy as np
from tqdm import trange


def make_normalized_random_vector(dims) -> list[float]:
    v = np.random.rand(dims)
    normalized = v / np.linalg.norm(v)
    return normalized.tolist()


def generate_values(count, create_time):
    return [
        (
            str(uuid.uuid4()),
            make_normalized_random_vector(1536),
            create_time,
            time.time(),
            0,
        )
        for _ in range(count)
    ]


def time_of_next_month(year, month):
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return year, month, time.mktime((year, month, 1, 12, 0, 0, 0, 0, 0))


async def run_query(count, batch, query_func):
    start_time = time.perf_counter()
    for i in trange(0, count, batch):
        crs = []
        for j in range(batch):
            vector = make_normalized_random_vector(1536)
            crs.append(query_func(vector))
        await asyncio.wait_for(asyncio.gather(*crs), timeout=1000)
    time_cost = time.perf_counter() - start_time
    print(
        f"query {count} times, cost {time_cost:.3f} seconds, average {(time_cost / count):.3f} seconds"
    )


async def run_insert(total, batch_size, partition_size, insert_func):
    year, month, create_time = time_of_next_month(2000, 0)
    print("start with partition %d%02d" % (year, month))
    for i in range(0, total, batch_size):
        if i > 0 and i % partition_size == 0:
            year, month, create_time = time_of_next_month(year, month)
            print("new partition %d%02d" % (year, month))

        values = generate_values(batch_size, create_time)
        print("    insert record from %d to %d" % (i, i + batch_size))
        await insert_func(values)


async def run_insert_query(insert_func, query_func, result_func):
    values = generate_values(1, time.time())
    await insert_func(values)
    uuid, vector = values[0][0], values[0][1]
    result = await query_func(vector)
    found = False
    for point_id in result_func(result):
        if point_id == uuid:
            print("found just inserted uuid in search")
            found = True
            break
    if not found:
        print("can not found just inserted uuid in search")
