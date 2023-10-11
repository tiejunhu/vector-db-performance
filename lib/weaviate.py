import json

import aiohttp
import weaviate

from . import utils


async def _search(url, collection, vector):
    query = """{
        Get {
            %s (
                limit: 5,
                nearVector: {
                    vector: %s
                }
            ) {
                update_time
                create_time
                is_deleted
                _additional {
                    distance
                    id
                }
            }
        }
    }""" % (
        collection.capitalize(),
        vector,
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url + "/v1/graphql",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
        ) as response:
            return await response.json()


def _insert_values(client, collection, values):
    client.batch.configure(batch_size=100, num_workers=4)
    with client.batch as batch:
        for value in values:
            batch.add_data_object(
                {
                    "create_time": value[2],
                    "update_time": value[3],
                    "is_deleted": value[4] == 1,
                },
                collection,
                uuid=value[0],
                vector=value[1],
            )


class Weaviate:
    def __init__(self, url, collection):
        self.url = url
        self.collection = collection
        self.client = weaviate.Client(url=url)

    async def init(self, drop):
        if drop:
            self.client.schema.delete_class(self.collection)
        class_obj = {
            "class": self.collection,
            "properties": [
                {"name": "create_time", "dataType": ["number"]},
                {"name": "update_time", "dataType": ["number"]},
                {"name": "is_deleted", "dataType": ["boolean"]},
            ],
            "vectorIndexConfig": {
                "distance": "cosine",
            },
        }
        self.client.schema.create_class(class_obj)

    async def load(self, total, batch_size, partition_size):
        async def insert(values):
            _insert_values(self.client, self.collection, values)

        await utils.run_insert(total, batch_size, partition_size, insert)

    async def query(self, count, batch):
        async def search(vector):
            await _search(self.url, self.collection, vector)

        await utils.run_query(count, batch, search)

    async def insert_and_query(self):
        async def insert(values):
            _insert_values(self.client, self.collection, values)

        async def search(vector):
            return await _search(self.url, self.collection, vector)

        def result_func(result):
            for point in result["data"]["Get"][self.collection.capitalize()]:
                yield point["_additional"]["id"]

        await utils.run_insert_query(insert, search, result_func)
