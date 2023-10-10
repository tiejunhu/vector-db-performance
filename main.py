import asyncio

import click

from lib.clickhouse import ClickHouse
from lib.qdrant import Qdrant


@click.group()
@click.option("--qdrant", default=False, is_flag=True, help="use qdrant")
@click.option("--clickhouse", default=False, is_flag=True, help="use clickhouse")
@click.option("--url", default="", help="url to qdrant or clickhouse")
@click.option("--database", default="test", help="default database name")
@click.pass_context
def cli(ctx, qdrant, clickhouse, url, database):
    ctx.ensure_object(dict)
    ctx.obj["QDRANT"] = qdrant
    ctx.obj["CLICKHOUSE"] = clickhouse
    ctx.obj["URL"] = url
    ctx.obj["DATABASE"] = database

    if qdrant and clickhouse:
        raise click.ClickException("You can't use both qdrant and clickhouse")
    if not qdrant and not clickhouse:
        raise click.ClickException("You must use qdrant or clickhouse")


def get_impl(ctx):
    url = ctx.obj["URL"]
    database = ctx.obj["DATABASE"]
    if ctx.obj["QDRANT"]:
        return Qdrant(url or "http://127.0.0.1:6333", database)
    if ctx.obj["CLICKHOUSE"]:
        return ClickHouse(url or "http://127.0.0.1:18123", database)

    raise click.ClickException("You must use qdrant or clickhouse")


@cli.command("init")
@click.pass_context
def init(ctx):
    impl = get_impl(ctx)
    asyncio.run(impl.init())


@cli.command("load")
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
    asyncio.run(impl.query(count, batch))


cli()
