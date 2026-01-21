import asyncio
import inspect
import json
import logging
from collections import defaultdict
from typing import Any, Awaitable, DefaultDict, Dict, Optional, Set
from uuid import uuid4

import redis.asyncio as aioredis
from fastapi import WebSocket
from redis import Redis

from .redis_client import get_redis


class ConnectionManager:
    def __init__(self):
        self.active_connections: DefaultDict[str, Set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, doc_id: str, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active_connections[doc_id].add(ws)

    async def disconnect(self, doc_id: str, ws: WebSocket):
        async with self._lock:
            self.active_connections[doc_id].discard(ws)
            if not self.active_connections[doc_id]:
                del self.active_connections[doc_id]

    async def broadcast_local(self, doc_id: str, message: dict):
        """Broadcast message to all local clients connected to this doc."""
        conns = list(self.active_connections.get(doc_id, []))
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception:
                await self.disconnect(doc_id, ws)


conn_manager = ConnectionManager()


logger = logging.getLogger("redis_pubsub")
logger.setLevel(logging.INFO)

# Helper types
ChannelName = str
MessagePayload = dict[str, Any]


class LocalConnection:
    """Wrapper for per-connection info — can be extended (user id, perms, etc.)"""

    def __init__(
        self,
        websocket: WebSocket,
        connection_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ):
        self.websocket = websocket
        self.id = connection_id or str(uuid4())
        self._send_lock = asyncio.Lock()  # avoid concurrent websocket.send_json calls
        self.user_id = user_id

    async def send_json(self, payload: MessagePayload):
        """Send JSON to client, protected with lock to avoid concurrency errors"""
        async with self._send_lock:
            await self.websocket.send_json(payload)


class RedisPubSubManager:
    """
    Manager that attaches local WebSocket connections to Redis channels.

    Behavior:
    - When the first local connection subscribes to a channel, we start a Redis PubSub
        listener task for that channel.
    - When the last local connection leaves, we cancel the Redis listener for that channel.
    - publish(...) writes to Redis so other instances also receive the message.
    - on Redis messages, manager broadcasts to all local connections for that channel.

    This provides a scalable pattern: multiple app instances rely on Redis as the fan-out hub.
    """

    def __init__(
        self,
        redis_kwargs: Optional[dict[str, Any]] = None,
        max_queue_size: int = 1000,
    ):
        self._redis_kwargs = redis_kwargs or {}
        self._redis: Optional[aioredis.Redis] = None

        # map channel -> set of LocalConnection
        self._local_subscribers: dict[ChannelName, Set[LocalConnection]] = {}

        # map channel -> asyncio.Task (the redis listener)
        self._listener_tasks: dict[ChannelName, asyncio.Task] = {}

        # an internal queue size limit for broadcasting
        self._max_queue_size = max_queue_size

        # single mutex to protect subscription changes
        self._lock = asyncio.Lock()

        # shutdown event
        self._shutdown = asyncio.Event()

    async def connect(self):
        """Create Redis connection"""
        if self._redis:
            return
        self._redis = get_redis(decode_responses=True, **self._redis_kwargs)

    async def close(self):
        """Graceful shutdown: cancel listeners and close redis"""
        self._shutdown.set()

        for ch, task in list(self._listener_tasks.items()):
            logger.info("Cancelling listener for channel %s", ch)
            task.cancel()
        # wait a bit for tasks to finish
        await asyncio.sleep(0.1)
        if self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None
        logger.info("RedisPubSubManager closed")

    def _channel_name(self, channel_id: str) -> ChannelName:
        return f"channel:{channel_id}"

    async def publish(self, channel_id: str, payload: MessagePayload):
        """
        Publish payload to Redis channel.
        Best practice: persist the message in DB BEFORE calling publish().
        """
        if not self._redis:
            await self.connect()

        channel = self._channel_name(channel_id)
        serialized = json.dumps(payload, default=str)

        try:
            # fire-and-forget publish
            assert self._redis is not None

            await self._redis.publish(channel, serialized)
            logger.debug("Published to %s: %s", channel, serialized)
        except Exception:
            logger.exception("Publish failed for channel %s", channel)
            # Optional: implement retry/backoff here in production

    async def subscribe_local(
        self, channel_id: str, websocket: WebSocket, user_id: Optional[str] = None
    ) -> LocalConnection:
        """
        Register this local WebSocket to receive messages for channel_id.
        Starts a Redis listener for the channel if needed.
        Returns a LocalConnection object (useful for unsubscribing).
        """
        if not self._redis:
            await self.connect()

        channel = self._channel_name(channel_id)
        conn = LocalConnection(websocket)

        async with self._lock:
            subs = self._local_subscribers.setdefault(channel, set())
            subs.add(conn)
            logger.info(
                "Local connection %s added to %s (local count=%d)",
                conn.id,
                channel,
                len(subs),
            )

            # if this is the first local subscriber, start Redis listener
            if channel not in self._listener_tasks:
                task = asyncio.create_task(self._redis_listener_loop(channel))
                self._listener_tasks[channel] = task
                logger.info("Started Redis listener task for %s", channel)

        return conn

    async def unsubscribe_local(self, channel_id: str, conn: LocalConnection):
        """Remove the local connection. If no local subscribers remain we stop listening to Redis."""
        channel = self._channel_name(channel_id)
        async with self._lock:
            subs = self._local_subscribers.get(channel)
            if not subs:
                return
            subs.discard(conn)
            logger.info(
                "Local connection %s removed from %s (local count=%d)",
                conn.id,
                channel,
                len(subs),
            )
            if not subs:
                # stop listener
                task = self._listener_tasks.pop(channel, None)
                if task:
                    task.cancel()
                    logger.info(
                        "Cancelled Redis listener for %s (no local subscribers)",
                        channel,
                    )
                # clean up dict entry
                self._local_subscribers.pop(channel, None)

    async def get_channel_connected_users(self, channel: ChannelName) -> Set[str]:
        """Get the list of connected users for a channel."""
        conns = self._local_subscribers.get(channel, set())
        return {conn.user_id for conn in conns if conn.user_id}

    async def _broadcast_to_local(self, channel: ChannelName, payload: MessagePayload):
        """Broadcast payload to all local subscribers of the channel."""
        subs = self._local_subscribers.get(channel)
        if not subs:
            logger.debug("No local subscribers for %s", channel)
            return

        # Make a snapshot to avoid concurrency issues
        connections = list(subs)
        logger.debug("Broadcasting to %d local conns on %s", len(connections), channel)

        # Send concurrently but bounded
        send_tasks = []
        for conn in connections:
            # Optionally add per-connection filters/permissions here
            send_tasks.append(asyncio.create_task(self._safe_send(conn, payload)))

        # Wait for all sends but protect overall latency
        await asyncio.gather(*send_tasks, return_exceptions=True)

    async def _safe_send(self, conn: LocalConnection, payload: MessagePayload):
        """
        Send into websocket. Catch and ignore disconnected websockets.
        If a client is slow or unresponsive, this doesn't block the manager forever because
        each send is independently awaited (and underlying websocket.send may raise).
        """
        try:
            await conn.send_json(payload)
        except Exception as e:
            logger.warning("Failed to send to local conn %s: %s", conn.id, e)
            # If send fails due to closed connection, best to let app code cleanup (unsubscribe_local)
            # Optional: attempt to close websocket
            try:
                await conn.websocket.close()
            except Exception:
                pass

    async def _redis_listener_loop(self, channel: ChannelName):
        """
        Listener task that subscribes to a Redis channel and forwards messages to local subscribers.
        This task runs until cancelled or on shutdown. If Redis connection fails, attempts reconnect with backoff.
        """
        # Create a dedicated PubSub object so we can unsubscribe/close cleanly
        backoff = 0.5
        max_backoff = 5.0
        while not self._shutdown.is_set():
            try:
                # create a new PubSub object for this listener
                assert self._redis is not None
                pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
                await pubsub.subscribe(channel)
                logger.info("Subscribed to Redis channel %s", channel)

                # reset backoff on success
                backoff = 0.5

                async for message in pubsub.listen():
                    if message is None:
                        continue
                    # message is a dict: {'type': 'message', 'pattern': None, 'channel': 'chat:1', 'data': '...'}
                    data = message.get("data")
                    if data is None:
                        continue
                    # Try to parse JSON
                    try:
                        payload = json.loads(data)
                    except Exception:
                        logger.exception(
                            "Failed to parse message from redis for channel %s: %r",
                            channel,
                            data,
                        )
                        continue

                    # Broadcast to local subscribers
                    await self._broadcast_to_local(channel, payload)

                # if we break out of async for, close pubsub and loop to reconnect
                try:
                    await pubsub.unsubscribe(channel)
                    await pubsub.close()
                except Exception:
                    pass

            except asyncio.CancelledError:
                logger.info("Listener task for %s cancelled", channel)
                break
            except Exception as e:
                logger.exception(
                    "Redis listener for %s crashed: %s; reconnecting after %s seconds",
                    channel,
                    e,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)
                # try reconnecting (ensure redis connection is alive)
                try:
                    if not self._redis:
                        await self.connect()
                    else:
                        ping = self._redis.ping()
                        if inspect.isawaitable(ping):
                            await ping
                except Exception:
                    # force reconnect by re-creating client
                    try:
                        if self._redis:
                            await self._redis.close()
                    finally:
                        self._redis = None
                        try:
                            await self.connect()
                        except Exception:
                            logger.warning("Reconnect attempt failed")
            # loop to retry
        logger.info("Exiting listener loop for %s", channel)


manager = RedisPubSubManager()
