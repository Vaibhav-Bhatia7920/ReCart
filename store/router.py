import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.deps import get_session, redis_dep
from store.exceptions import ConflictError, NotFoundError, StoreError, ValidationError
from store.schemas import (
    AddItemRequest,
    AddressWrite,
    ApplyOfferRequest,
    CartRead,
    OrderRead,
    PaymentLinkRead,
    StoreSnapshot,
)
from store.services import (
    add_item,
    apply_offer,
    create_cart,
    create_payment_link,
    force_abandon_cart,
    get_cart,
    get_snapshot,
    mark_order_paid,
    update_address,
)

router = APIRouter()
SessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[Redis, Depends(redis_dep)]


def _http_error(exc: StoreError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=exc.message)
    if isinstance(exc, ConflictError):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.message)
    if isinstance(exc, ValidationError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message)
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message)


@router.post("/carts", response_model=CartRead, status_code=status.HTTP_201_CREATED)
async def create_cart_endpoint(session: SessionDep) -> CartRead:
    return await create_cart(session)


@router.get("/carts/{cart_id}", response_model=CartRead)
async def get_cart_endpoint(cart_id: uuid.UUID, session: SessionDep) -> CartRead:
    try:
        return await get_cart(session, cart_id)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.post("/carts/{cart_id}/items", response_model=CartRead)
async def add_item_endpoint(
    cart_id: uuid.UUID, payload: AddItemRequest, session: SessionDep
) -> CartRead:
    try:
        return await add_item(session, cart_id, payload.product_id, payload.quantity)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.post("/carts/{cart_id}/offers", response_model=CartRead)
async def apply_offer_endpoint(
    cart_id: uuid.UUID, payload: ApplyOfferRequest, session: SessionDep
) -> CartRead:
    try:
        return await apply_offer(session, cart_id, payload.code)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.put("/carts/{cart_id}/address", response_model=CartRead)
async def update_address_endpoint(
    cart_id: uuid.UUID, payload: AddressWrite, session: SessionDep
) -> CartRead:
    try:
        return await update_address(session, cart_id, payload)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.post("/carts/{cart_id}/payment-link", response_model=PaymentLinkRead)
async def create_payment_link_endpoint(
    cart_id: uuid.UUID, session: SessionDep
) -> PaymentLinkRead:
    try:
        return await create_payment_link(session, cart_id)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.post("/test/orders/{order_id}/mark-paid", response_model=OrderRead)
async def mark_order_paid_endpoint(order_id: uuid.UUID, session: SessionDep) -> OrderRead:
    try:
        return await mark_order_paid(session, order_id)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.post("/test/carts/{cart_id}/abandon", response_model=StoreSnapshot)
async def force_abandon_endpoint(
    cart_id: uuid.UUID, session: SessionDep, redis: RedisDep
) -> StoreSnapshot:
    try:
        return await force_abandon_cart(session, redis, cart_id)
    except StoreError as exc:
        raise _http_error(exc) from exc


@router.get("/test/carts/{cart_id}/snapshot", response_model=StoreSnapshot)
async def snapshot_endpoint(cart_id: uuid.UUID, session: SessionDep) -> StoreSnapshot:
    try:
        return await get_snapshot(session, cart_id)
    except StoreError as exc:
        raise _http_error(exc) from exc
