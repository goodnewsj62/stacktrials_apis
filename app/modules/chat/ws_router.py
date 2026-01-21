import asyncio
import traceback
from typing import TYPE_CHECKING, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, WebSocketException
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.constants import PER_PAGE
from app.common.utils import chat_history_ws_channel, websocket_error_wrapper
from app.common.ws_manager import RedisPubSubManager, manager
from app.core.dependencies import CurrentWSUser, SessionDep
from app.modules.chat.service import ChatService
from app.schemas.chat import (
    ChatMessageRead,
    ChatMessageUpdate,
    ChatMessageWrite,
    ChatRead,
    PaginatedMessages,
)

if TYPE_CHECKING:
    from app.common.ws_manager import LocalConnection

router = APIRouter()


@router.websocket("/")
async def connect_chat_histories(
    websocket: WebSocket,
    session: SessionDep,
    current_user: CurrentWSUser,
    q: Optional[str] = None,
    page: Optional[int] = None,
):
    await websocket.accept()
    initial_data = await ChatService.list_chat(
        session, current_user, page or 1, PER_PAGE, q
    )

    initial_data = [
        {
            **item,
            "chat": item["chat"].model_dump(mode="json"),
            "last_message": (
                item["last_message"].model_dump(mode="json")
                if item["last_message"]
                else None
            ),
        }
        for item in initial_data["items"]
    ]
    await websocket.send_json({"event": "chat.list", "data": initial_data})

    sub_key = chat_history_ws_channel(current_user)
    conns: dict[str, LocalConnection] = {}
    local_conn = await manager.subscribe_local(sub_key, websocket)
    conns[sub_key] = local_conn

    async def cleanup_all_connections():
        """Clean up all connections with timeout protection and parallel execution"""
        if not conns:
            return

        # Create cleanup tasks for all connections in parallel
        cleanup_tasks = []
        for key_, con in list(conns.items()):
            try:
                # Wrap each unsubscribe with a timeout
                task = asyncio.wait_for(
                    manager.unsubscribe_local(key_, con),
                    timeout=2.0,  # 2 second timeout per connection
                )
                cleanup_tasks.append(task)
            except Exception:
                pass  # If task creation fails, continue with others

        # Execute all cleanups in parallel with exception handling
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    try:
        while True:
            try:
                raw_data = await websocket.receive_json()
            except WebSocketDisconnect:
                # Re-raise disconnect to outer handler
                raise
            except Exception:
                # Only catch parsing/validation errors, not disconnects
                continue

            event = raw_data.get("event")
            data = raw_data.get("data")

            if event == "chat.subscribe":

                if not isinstance(data, str):
                    raise WebSocketException(1002, "data must be a chat_id in string")
                con = await manager.subscribe_local(data, websocket, current_user.id)
                conns[data] = con
            elif event == "chat.unsubscribe":
                if not isinstance(data, str):
                    raise WebSocketException(1002, "data must be a chat_id in string")
                await manager.unsubscribe_local(data, conns[data])
                conns.pop(data, None)  # Remove from tracking

    except WebSocketDisconnect:
        # Clean up all connections on disconnect
        await cleanup_all_connections()
    except Exception as exc:
        # logger.exception("Exception in websocket handler: %s", exc)
        # ensure cleanup on any other error
        await cleanup_all_connections()
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/{chat_id}")
async def connect_to_chat(
    websocket: WebSocket, chat_id: str, session: SessionDep, current_user: CurrentWSUser
):
    """
    user comes in
    we check if the chat id exists
    if yes we get if the the user is an active member of the chat
    if yes we then proceed to fetch the initial data that is page one data
    after which we open the websocket for the user
    """

    await websocket.accept()
    initial_data = await ChatService.get_initial_data(chat_id, session, current_user)

    await websocket.send_json(
        {
            "event": "chat.initial",
            "data": PaginatedMessages.model_validate(initial_data).model_dump(
                mode="json"
            ),
        }
    )

    local_conn = await manager.subscribe_local(chat_id, websocket, current_user.id)
    sync_key = chat_history_ws_channel(current_user)
    sync_conn = await manager.subscribe_local(sync_key, websocket)
    try:
        while True:
            try:
                raw_data = await websocket.receive_json()
            except WebSocketDisconnect:
                # Re-raise disconnect to outer handler
                raise
            except Exception:
                # Only catch parsing/validation errors, not disconnects
                continue

            event = raw_data.get("event")
            data = raw_data.get("data")

            if event == "chat.message.create":

                resp = await websocket_error_wrapper(
                    ChatService.create_message,
                    session,
                    current_user,
                    ChatMessageWrite.model_validate(data),
                )

                model = ChatMessageRead.model_validate(resp)

                await manager.publish(
                    chat_id,
                    {
                        "event": "chat.message.create",
                        "data": model.model_dump(mode="json"),
                    },
                )

            elif event == "chat.message.update":
                resp = await websocket_error_wrapper(
                    ChatService.update_message,
                    session,
                    current_user,
                    data.get("message_id"),
                    ChatMessageUpdate.model_validate(data, extra="ignore"),
                )
                model = ChatMessageRead.model_validate(resp)
                await manager.publish(
                    chat_id,
                    {
                        "event": "chat.message.update",
                        "data": model.model_dump(mode="json"),
                    },
                )
            elif event == "chat.message.delete":
                if not isinstance(data, str):
                    raise WebSocketException(1002, "data must be a string")
                resp = await websocket_error_wrapper(
                    ChatService.delete_message, session, current_user, data
                )
                model = ChatMessageRead.model_validate(resp)
                await manager.publish(
                    chat_id,
                    {
                        "event": "chat.message.delete",
                        "data": model.model_dump(mode="json"),
                    },
                )
            elif event == "chat.update":
                resp = await websocket_error_wrapper(
                    ChatService.update_chat, session, current_user, chat_id, data
                )
                model = ChatRead.model_validate(resp)
                await manager.publish(
                    chat_id,
                    {"event": "chat.update", "data": model.model_dump(mode="json")},
                )
                await manager.publish(
                    sync_key,
                    {"event": "chat.update", "data": model.model_dump(mode="json")},
                )
                # TODO: The update to the chat should be broadcast to the chat members
            elif event == "chat.reaction.create" or event == "chat.reaction.delete":
                message_id = raw_data.get("message_id")
                if not message_id:
                    raise WebSocketException(1002, "message_id must be present")

                resp = await websocket_error_wrapper(
                    ChatService.create_delete_reaction,
                    session,
                    current_user,
                    message_id,
                    data,
                )
                model = ChatMessageRead.model_validate(resp)
                await manager.publish(
                    chat_id,
                    {
                        "event": "chat.message.update",
                        "data": model.model_dump(mode="json"),
                    },
                )
            elif event == "chat.member.delete":
                if isinstance(data, str):
                    raise WebSocketException(1002, "data must be a string")
                resp = await websocket_error_wrapper(
                    ChatService.remove_member, session, current_user, chat_id, data
                )
                await manager.publish(
                    chat_id,
                    {"event": "chat.member.delete", "data": model.model_dump_json()},
                )
            # END OF EVENTS

            await broadcast_stat_updates_smart(
                manager, session, chat_id, current_user.id
            )
            stat_resp = (
                {
                    "event": "chat.stat",
                    "data": {
                        **await ChatService.fetch_one_unread_stats(
                            session, chat_id, current_user.id
                        ),
                        "chat_id": chat_id,
                    },
                },
            )
            await manager.publish(chat_id, stat_resp)
            await manager.publish(sync_key, stat_resp)

    except WebSocketDisconnect:
        # Clean up with timeout protection
        try:
            await asyncio.wait_for(
                manager.unsubscribe_local(chat_id, local_conn), timeout=2.0
            )
        except Exception:
            pass  # Cleanup failed, but don't block
    except Exception as exc:
        # logger.exception("Exception in websocket handler: %s", exc)
        # ensure cleanup

        print("-- catch all error -----", exc)
        print(traceback.format_exc())
        try:
            await asyncio.wait_for(
                manager.unsubscribe_local(chat_id, local_conn), timeout=2.0
            )
        except Exception:
            pass  # Cleanup failed, but don't block
        try:
            await websocket.close()
        except Exception:
            pass


async def broadcast_stat_updates_smart(
    manager: RedisPubSubManager,
    session: AsyncSession,
    chat_id: str,
    current_user_id: str,
):
    """
    Smart stat updates - only to users who need them immediately.
    """

    connected_user_ids = await manager.get_channel_connected_users(chat_id)

    print("connected_user_ids", connected_user_ids)

    if not connected_user_ids:
        return

    target_users = [uid for uid in connected_user_ids if uid != current_user_id]

    user_stats = await ChatService.fetch_unread_stats_for_users(
        session, chat_id, target_users
    )

    print("user_stats", user_stats)

    # 5. Publish in parallel (non-blocking)
    tasks = []
    for user_id, stats in user_stats.items():
        sync_key = chat_history_ws_channel(user_id)
        task = manager.publish(
            sync_key,
            {
                "event": "chat.stat",
                "data": {
                    "chat_id": chat_id,
                    "unread_count": stats["unread_count"],
                    "has_reply": stats["has_reply"],
                    "last_message": stats["last_message"],
                },
            },
        )
        tasks.append(task)

    # Execute all publishes in parallel
    await asyncio.gather(*tasks, return_exceptions=True)


############ Future scale implementation ###########
# 1. redis will be used to store chat memebers and unreas count
# When message created:


# async def increment_unread_for_chat(chat_id: str, sender_id: str):
#     """
#     Increment unread count in Redis for all members except sender.
#     O(1) operation per user using Redis sorted sets.
#     """
#     # Get member IDs from cache (not DB)
#     member_ids = await redis.smembers(f"chat:{chat_id}:members")

#     # Batch increment in Redis (VERY fast)
#     pipe = redis.pipeline()
#     for member_id in member_ids:
#         if member_id != sender_id:
#             pipe.hincrby(f"user:{member_id}:unread", chat_id, 1)
#     await pipe.execute()

#     # Optionally: Publish lightweight event
#     await manager.publish(
#         f"chat:{chat_id}:activity",
#         {"event": "chat.activity", "data": {"chat_id": chat_id}}


# 2. hybrid approach:
# async def handle_new_message(chat_id, message, sender_id, mentioned_ids):
#     """
#     Multi-tier notification strategy
#     """

#     # Tier 1: Active viewers get full message immediately
#     await manager.publish(
#         chat_id,
#         {"event": "chat.message.create", "data": message}
#     )

#     # Tier 2: Mentioned users get high-priority notification
#     if mentioned_ids:
#         for user_id in mentioned_ids:
#             sync_key = chat_history_ws_channel(user_id)
#             await manager.publish(
#                 sync_key,
#                 {
#                     "event": "chat.mention",
#                     "data": {
#                         "chat_id": chat_id,
#                         "message_id": message.id,
#                     }
#                 }
#             )

#     # Tier 3: Update Redis counters (fast, async)
#     asyncio.create_task(
#         update_redis_unread_counts(chat_id, sender_id)
#     )

#     # Tier 4: Connected users get stat updates (batched)
#     asyncio.create_task(
#         broadcast_to_connected_users_only(chat_id, sender_id)
#     )
