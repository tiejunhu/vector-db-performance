import asyncio

import click

from lib.clickhouse import ClickHouse
from lib.qdrant import Qdrant
from lib.weaviate import Weaviate


async def wrap_close(impl, func):
    await func
    await impl.close()


@click.group()
@click.option("--qdrant", default=False, is_flag=True, help="use qdrant")
@click.option("--clickhouse", default=False, is_flag=True, help="use clickhouse")
@click.option("--weaviate", default=False, is_flag=True, help="use weaviate")
@click.option("--url", default="", help="url to qdrant or clickhouse or weaviate")
@click.option("--database", default="test", help="database name, default is test")
@click.pass_context
def cli(ctx, qdrant, clickhouse, weaviate, url, database):
    ctx.ensure_object(dict)
    ctx.obj["QDRANT"] = qdrant
    ctx.obj["CLICKHOUSE"] = clickhouse
    ctx.obj["WEAVIATE"] = weaviate
    ctx.obj["URL"] = url
    ctx.obj["DATABASE"] = database

    databases = [qdrant, clickhouse, weaviate].count(True)
    if databases == 0:
        raise click.ClickException("You must specify a database")
    if databases > 1:
        raise click.ClickException("You must specify only one database")


def get_impl(ctx):
    url = ctx.obj["URL"]
    database = ctx.obj["DATABASE"]
    if ctx.obj["QDRANT"]:
        return Qdrant(url or "http://127.0.0.1:6333", database)
    if ctx.obj["CLICKHOUSE"]:
        return ClickHouse(url or "http://127.0.0.1:18123", database)
    if ctx.obj["WEAVIATE"]:
        return Weaviate(url or "http://127.0.0.1:8080", database)

    raise click.ClickException("You must use qdrant or clickhouse")


@cli.command("init")
@click.option("--drop", default=False, is_flag=True, help="drop database before init")
@click.pass_context
def init(ctx, drop):
    impl = get_impl(ctx)
    asyncio.run(impl.init(drop))


@cli.command("load", help="load random records to database")
@click.option("--count", default=1000000, help="records to generate")
@click.option("--batch", default=1000, help="batch size")
@click.option("--partition", default=10000, help="partition size")
@click.pass_context
def load(ctx, count, batch, partition):
    if batch > count:
        batch = count
    if partition > count:
        partition = count
    impl = get_impl(ctx)
    asyncio.run(impl.load(count, batch, partition))


@cli.command("query")
@click.option("--count", default=10, help="times to repeat")
@click.option("--batch", default=1, help="batch size")
@click.pass_context
def query(ctx, count, batch):
    impl = get_impl(ctx)

    asyncio.run(wrap_close(impl, impl.query(count, batch)))


@cli.command(
    "index", help="create a record and query it immediately to test if index is ready"
)
@click.pass_context
def query(ctx):
    impl = get_impl(ctx)

    asyncio.run(wrap_close(impl, impl.insert_and_query()))


cli()
