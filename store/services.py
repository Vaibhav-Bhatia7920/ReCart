import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.settings import get_settings
from store.exceptions import ConflictError, NotFoundError, ValidationError
from store.models import (
    Address,
    Cart,
    CartItem,
    CartStatus,
    Offer,
    Order,
    OrderStatus,
    Product,
)
from store.pricing import compute_totals, money, offer_is_valid, touch_cart
from store.schemas import (
    AddressRead,
    AddressWrite,
    CartItemRead,
    CartRead,
    OfferRead,
    OrderRead,
    PaymentLinkRead,
    StoreSnapshot,
    TotalsRead,
)

_CART_LOAD = (
    selectinload(Cart.items).selectinload(CartItem.product),
    selectinload(Cart.address),
    selectinload(Cart.applied_offer),
    selectinload(Cart.orders),
)


def _delivery_fee() -> Decimal:
    return money(get_settings().store_delivery_fee)


async def _get_cart(session: AsyncSession, cart_id: uuid.UUID) -> Cart:
    result = await session.execute(select(Cart).options(*_CART_LOAD).where(Cart.id == cart_id))
    cart = result.scalar_one_or_none()
    if cart is None:
        raise NotFoundError(f"Cart {cart_id} not found")
    return cart


async def _reload_cart(session: AsyncSession, cart_id: uuid.UUID) -> Cart:
    session.expire_all()
    return await _get_cart(session, cart_id)


def _require_active(cart: Cart) -> None:
    if cart.status is not CartStatus.ACTIVE:
        raise ConflictError(f"Cart is {cart.status.value}")


def _to_item_read(item: CartItem) -> CartItemRead:
    return CartItemRead(
        id=item.id,
        product_id=item.product_id,
        product_name=item.product.name,
        quantity=item.quantity,
        unit_price=item.unit_price,
        line_total=money(item.unit_price * item.quantity),
    )


def _to_offer_read(offer: Offer | None, subtotal: Decimal) -> OfferRead | None:
    if offer is None:
        return None
    return OfferRead(
        id=offer.id,
        code=offer.code,
        kind=offer.kind,
        percent_off=offer.percent_off,
        min_subtotal=offer.min_subtotal,
        valid=offer_is_valid(offer, subtotal),
    )


def _effective_offer(cart: Cart) -> Offer | None:
    subtotal, _, _, _ = compute_totals(cart.items, None, _delivery_fee())
    if cart.applied_offer is None:
        return None
    if offer_is_valid(cart.applied_offer, subtotal):
        return cart.applied_offer
    return None


def to_cart_read(cart: Cart) -> CartRead:
    offer = _effective_offer(cart)
    subtotal, discount, delivery_fee, total = compute_totals(
        cart.items, offer, _delivery_fee()
    )
    address = AddressRead.model_validate(cart.address) if cart.address is not None else None
    return CartRead(
        id=cart.id,
        status=cart.status,
        items=[_to_item_read(item) for item in cart.items],
        address=address,
        applied_offer=_to_offer_read(cart.applied_offer, subtotal),
        totals=TotalsRead(
            subtotal=subtotal,
            discount=discount,
            delivery_fee=delivery_fee,
            total=total,
        ),
        last_activity_at=cart.last_activity_at,
        abandonment_emitted_at=cart.abandonment_emitted_at,
    )


def to_order_read(order: Order) -> OrderRead:
    return OrderRead.model_validate(order)


def to_snapshot(cart: Cart) -> StoreSnapshot:
    orders = sorted(cart.orders, key=lambda row: row.created_at)
    return StoreSnapshot(cart=to_cart_read(cart), orders=[to_order_read(o) for o in orders])


async def create_cart(session: AsyncSession) -> CartRead:
    cart = Cart(status=CartStatus.ACTIVE, last_activity_at=datetime.now(UTC))
    session.add(cart)
    await session.flush()
    loaded = await _reload_cart(session, cart.id)
    return to_cart_read(loaded)


async def get_cart(session: AsyncSession, cart_id: uuid.UUID) -> CartRead:
    cart = await _get_cart(session, cart_id)
    return to_cart_read(cart)


async def add_item(
    session: AsyncSession,
    cart_id: uuid.UUID,
    product_id: uuid.UUID,
    quantity: int,
) -> CartRead:
    cart = await _get_cart(session, cart_id)
    _require_active(cart)
    product = await session.get(Product, product_id)
    if product is None:
        raise NotFoundError(f"Product {product_id} not found")
    if product.stock < quantity:
        raise ValidationError("Insufficient stock")

    existing = next((item for item in cart.items if item.product_id == product_id), None)
    if existing is None:
        session.add(
            CartItem(
                cart_id=cart.id,
                product_id=product.id,
                quantity=quantity,
                unit_price=money(product.price),
            )
        )
    else:
        existing.quantity += quantity
    product.stock -= quantity
    touch_cart(cart)
    await session.flush()
    cart = await _reload_cart(session, cart.id)
    return to_cart_read(cart)


async def apply_offer(session: AsyncSession, cart_id: uuid.UUID, code: str) -> CartRead:
    cart = await _get_cart(session, cart_id)
    _require_active(cart)
    result = await session.execute(select(Offer).where(Offer.code == code.upper()))
    offer = result.scalar_one_or_none()
    if offer is None:
        raise NotFoundError(f"Offer {code} not found")
    subtotal, _, _, _ = compute_totals(cart.items, None, _delivery_fee())
    if not offer_is_valid(offer, subtotal):
        raise ValidationError("Offer is not valid for this cart")
    cart.applied_offer_id = offer.id
    touch_cart(cart)
    await session.flush()
    cart = await _reload_cart(session, cart.id)
    return to_cart_read(cart)


async def update_address(
    session: AsyncSession, cart_id: uuid.UUID, payload: AddressWrite
) -> CartRead:
    cart = await _get_cart(session, cart_id)
    _require_active(cart)
    if cart.address is None:
        cart.address = Address(cart_id=cart.id, **payload.model_dump())
    else:
        for field, value in payload.model_dump().items():
            setattr(cart.address, field, value)
    touch_cart(cart)
    await session.flush()
    cart = await _reload_cart(session, cart.id)
    return to_cart_read(cart)


async def create_payment_link(session: AsyncSession, cart_id: uuid.UUID) -> PaymentLinkRead:
    cart = await _get_cart(session, cart_id)
    _require_active(cart)
    if not cart.items:
        raise ValidationError("Cart is empty")
    if cart.address is None:
        raise ValidationError("Address is required")
    offer = _effective_offer(cart)
    if cart.applied_offer is not None and offer is None:
        raise ValidationError("Applied offer is no longer valid")
    subtotal, discount, delivery_fee, total = compute_totals(
        cart.items, offer, _delivery_fee()
    )
    order = Order(
        cart_id=cart.id,
        status=OrderStatus.PENDING_PAYMENT,
        payment_url="",
        subtotal=subtotal,
        discount=discount,
        delivery_fee=delivery_fee,
        total=total,
    )
    session.add(order)
    await session.flush()
    order.payment_url = f"https://payments.example.test/orders/{order.id}"
    touch_cart(cart)
    await session.flush()
    return PaymentLinkRead(order=to_order_read(order), payment_url=order.payment_url)


async def mark_order_paid(session: AsyncSession, order_id: uuid.UUID) -> OrderRead:
    result = await session.execute(
        select(Order).options(selectinload(Order.cart)).where(Order.id == order_id)
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise NotFoundError(f"Order {order_id} not found")
    if order.status is not OrderStatus.PENDING_PAYMENT:
        raise ConflictError(f"Order is {order.status.value}")
    cart = await _get_cart(session, order.cart_id)
    _require_active(cart)
    order.status = OrderStatus.PAID
    order.paid_at = datetime.now(UTC)
    cart.status = CartStatus.CONVERTED
    await session.flush()
    return to_order_read(order)


async def restore_reserved_stock(session: AsyncSession, cart: Cart) -> None:
    for item in cart.items:
        product = await session.get(Product, item.product_id)
        if product is None:
            continue
        product.stock += item.quantity


async def publish_abandonment_once(
    session: AsyncSession,
    redis: Redis,
    cart: Cart,
) -> bool:
    """Mark emission and publish at most one Redis stream event for this cart."""
    settings = get_settings()
    if cart.abandonment_emitted_at is not None:
        return False
    key = f"{settings.redis_abandonment_emitted_prefix}:{cart.id}"
    acquired = await redis.set(key, "1", nx=True)
    if acquired:
        try:
            await redis.xadd(
                settings.redis_abandonment_stream,
                {
                    "cart_id": str(cart.id),
                    "status": CartStatus.ABANDONED.value,
                    "emitted_at": datetime.now(UTC).isoformat(),
                },
            )
        except Exception:
            await redis.delete(key)
            raise
    cart.abandonment_emitted_at = datetime.now(UTC)
    await session.flush()
    return bool(acquired)


async def abandon_cart(
    session: AsyncSession,
    redis: Redis,
    cart_id: uuid.UUID,
    *,
    skip_timeout: bool,
) -> StoreSnapshot:
    cart = await _get_cart(session, cart_id)
    if cart.status is CartStatus.CONVERTED:
        raise ConflictError("Converted carts cannot be abandoned")
    if cart.status is CartStatus.ACTIVE:
        settings = get_settings()
        idle_for = datetime.now(UTC) - cart.last_activity_at
        if not skip_timeout and idle_for.total_seconds() < settings.cart_abandonment_timeout_seconds:
            raise ConflictError("Cart is still within the abandonment timeout")
        await restore_reserved_stock(session, cart)
        for order in cart.orders:
            if order.status is OrderStatus.PENDING_PAYMENT:
                order.status = OrderStatus.CANCELLED
        cart.status = CartStatus.ABANDONED
        await session.flush()
    await publish_abandonment_once(session, redis, cart)
    cart = await _reload_cart(session, cart.id)
    return to_snapshot(cart)


async def force_abandon_cart(
    session: AsyncSession, redis: Redis, cart_id: uuid.UUID
) -> StoreSnapshot:
    return await abandon_cart(session, redis, cart_id, skip_timeout=True)


async def get_snapshot(session: AsyncSession, cart_id: uuid.UUID) -> StoreSnapshot:
    cart = await _get_cart(session, cart_id)
    return to_snapshot(cart)


async def sweep_abandoned_carts(session: AsyncSession, redis: Redis) -> int:
    settings = get_settings()
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.cart_abandonment_timeout_seconds)
    result = await session.execute(
        select(Cart)
        .options(*_CART_LOAD)
        .where(Cart.status == CartStatus.ACTIVE, Cart.last_activity_at <= cutoff)
        .with_for_update(skip_locked=True)
    )
    carts = list(result.scalars().unique())
    emitted = 0
    for cart in carts:
        snapshot = await abandon_cart(session, redis, cart.id, skip_timeout=True)
        if snapshot.cart.abandonment_emitted_at is not None:
            emitted += 1
    pending = await session.execute(
        select(Cart)
        .options(*_CART_LOAD)
        .where(
            Cart.status == CartStatus.ABANDONED,
            Cart.abandonment_emitted_at.is_(None),
        )
        .with_for_update(skip_locked=True)
    )
    for cart in pending.scalars().unique():
        published = await publish_abandonment_once(session, redis, cart)
        if published:
            emitted += 1
    return emitted
