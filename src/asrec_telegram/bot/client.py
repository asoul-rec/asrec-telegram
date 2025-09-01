from contextlib import asynccontextmanager
from pyrogram import Client
from asyncio import TaskGroup


@asynccontextmanager
async def app_group(apps: list[Client]):
    async with TaskGroup() as group:
        for _app in apps:
            group.create_task(_app.__aenter__())
    try:
        yield apps
    finally:
        async with TaskGroup() as group:
            for _app in apps:
                group.create_task(_app.__aexit__())


async def get_peer_id(client: Client, chat_id: int) -> int:
    from pyrogram.raw.types import InputPeerChannel, InputPeerChat, InputPeerUser
    peer = await client.resolve_peer(chat_id)
    if isinstance(peer, InputPeerUser):
        return peer.user_id
    elif isinstance(peer, InputPeerChat):
        return int(f"-{peer.chat_id}")
    elif isinstance(peer, InputPeerChannel):
        return int(f"-100{peer.channel_id}")
    else:
        raise RuntimeError(f"Unknown peer type from resolve_peer: {type(peer)}")
