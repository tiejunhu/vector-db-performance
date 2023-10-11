import asyncio
import time

from aiochclient import ChClient
from aiohttp import ClientSession, ClientTimeout
from tqdm import trange

from . import utils


def create_client(url, database):
    user = "default"
    password = ""
    timeout = ClientTimeout(total=100)
    return ChClient(ClientSession(timeout=timeout), url, user, password, database)


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tbl
(
    `uuid` String,
    `text_vector` Array(Float32) CODEC(Delta, ZSTD),
    `create_time` DateTime64 DEFAULT now64(),
    `update_time` DateTime64 DEFAULT now64(),
    `is_deleted` UInt8 DEFAULT 0,
)
    ENGINE = ReplacingMergeTree(update_time, is_deleted)
    PARTITION BY toYYYYMM(create_time)
    ORDER BY (uuid)
    SETTINGS index_granularity=64
"""


async def _query(self, query_vector):
    sql = (
        f"SELECT uuid, L2Distance(text_vector, {query_vector}) AS score "
        "FROM tbl WHERE is_deleted=0 "
        "ORDER BY score ASC LIMIT 5"
    )
    return await self._fetch(sql)


async def _insert(self, values):
    await self._exec(
        "INSERT INTO tbl (uuid, text_vector, create_time, update_time, is_deleted) VALUES",
        *values,
    )


class ClickHouse:
    def _set_database(self, db):
        self.client.params["database"] = db

    async def _exec(self, sql, *args, **kwargs):
        return await self.client.execute(sql, *args, params=kwargs)

    async def _fetch(self, sql, *args, **kwargs):
        return await self.client.fetch(sql, *args, params=kwargs)

    def __init__(self, url, database):
        self.url = url
        self.database = database
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = create_client(self.url, self.database)
        return self._client

    async def init(self, drop):
        self._set_database("default")
        await self._exec("CREATE DATABASE IF NOT EXISTS " + self.database)
        self._set_database(self.database)
        await self._exec(TABLE_SQL)
        print("database and table created")

    async def query(self, count, batch):
        async def query(query_vector):
            return await _query(self, query_vector)

        await utils.run_query(count, batch, query)

    async def load(self, total, batch_size, partition_size):
        async def insert(values):
            await _insert(self, values)

        await utils.run_insert(total, batch_size, partition_size, insert)

    async def insert_and_query(self):
        async def query(query_vector):
            return await _query(self, query_vector)

        async def insert(values):
            await _insert(self, values)

        def result_func(result):
            for row in result:
                yield row[0]

        await utils.run_insert_query(insert, query, result_func)

    async def close(self):
        if self._client is not None:
            await self._client.close()
