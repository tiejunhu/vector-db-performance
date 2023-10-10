import asyncio
import time
from typing import Optional

import qdrant_client.http.models as m
from qdrant_client.http import AsyncApis
from qdrant_client.http.api.collections_api import SyncCollectionsApi
from qdrant_client.http.api_client import SyncApis
from qdrant_client.http.exceptions import UnexpectedResponse
from tqdm import trange

from . import utils


def _create_collection_params(vector_dim: int):
    # default collection properties
    # https://our.ones.pro/wiki/#/team/RDjYMhKq/space/2qrMo6vL/page/QFsb34QJ
    return m.CreateCollection(
        hnsw_config=m.HnswConfigDiff(
            on_disk=True,
        ),
        vectors=m.VectorParams(
            size=vector_dim,
            distance=m.Distance.COSINE,
        ),
        optimizers_config=m.OptimizersConfigDiff(
            memmap_threshold=100000,
        ),
        quantization_config=m.ScalarQuantization(
            scalar=m.ScalarQuantizationConfig(
                type=m.ScalarType.INT8,
                always_ram=True,
            )
        ),
        on_disk_payload=True,
    )


def _get_collection_info(
    collections_api: SyncCollectionsApi, collection_name: str
) -> Optional[m.CollectionInfo]:
    """
    获取collection信息

    """
    try:
        result = collections_api.get_collection(collection_name=collection_name)
        return result.result
    except UnexpectedResponse as e:
        if e.status_code == 404:
            return None
        raise e


def _create_collection(
    collections_api: SyncCollectionsApi, collection_name: str, vector_dim: int
):
    print(f"creating collection {collection_name}")
    collections_api.create_collection(
        collection_name=collection_name,
        create_collection=_create_collection_params(vector_dim),
    )


async def _batch_insert(async_client, collection, points):
    await async_client.points_api.upsert_points(
        collection_name=collection,
        point_insert_operations=m.PointsList(
            points=points,
        ),
    )


async def _insert_values(async_client, collection, values):
    points = [
        m.PointStruct(
            id=v[0],
            vector=v[1],
            payload={
                "create_time": v[2],
                "update_time": v[3],
                "is_deleted": v[4],
            },
        )
        for v in values
    ]
    await _batch_insert(async_client, collection, points)


async def _search(async_client, collection, vector):
    search_params = m.SearchParams(
        hnsw_ef=256,
        quantization=m.QuantizationSearchParams(
            oversampling=2,
            rescore=True,
            ignore=False,
        ),
    )
    search_request = m.SearchRequest(
        vector=vector,
        limit=5,
        with_vector=False,
        with_payload=True,
        params=search_params,
    )
    return await async_client.points_api.search_points(
        collection_name=collection,
        search_request=search_request,
    )


class Qdrant:
    def _prepare_for_create(self):
        client = SyncApis(host=self.url)
        collection = _get_collection_info(client.collections_api, self.collection)
        if collection is None:
            _create_collection(client.collections_api, self.collection, 1536)
        else:
            print(f"collection {self.collection} already exists, skipping create")
        client.close()

    def __init__(self, url, collection):
        self.url = url
        self.collection = collection
        self.async_client = AsyncApis(self.url)

    async def init(self):
        self._prepare_for_create()

    async def query(self, count, batch):
        async def search(vector):
            return await _search(self.async_client, self.collection, vector)

        await utils.run_query(count, batch, search)

    async def load(self, total, batch_size, partition_size):
        async def insert(values):
            await _insert_values(self.async_client, self.collection, values)

        await utils.run_insert(total, batch_size, partition_size, insert)

    async def insert_and_query(self):
        async def insert(values):
            await _insert_values(self.async_client, self.collection, values)

        async def search(vector):
            return await _search(self.async_client, self.collection, vector)

        def result_func(result):
            for point in result.result:
                yield point.id

        await utils.run_insert_query(insert, search, result_func)
