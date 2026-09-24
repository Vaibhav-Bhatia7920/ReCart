from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_redis
from app.settings import get_settings
from store.models import Cart, CartStatus, OrderStatus, Product
from store.services import sweep_abandoned_carts

ADDRESS = {
    "line1": "12 Market Street",
    "line2": None,
    "city": "Austin",
    "region": "TX",
    "postal_code": "78701",
    "country": "US",
}


async def _create_cart(client: AsyncClient) -> dict[str, Any]:
    response = await client.post("/carts")
    assert response.status_code == 201
    return response.json()


async def _add_item(
    client: AsyncClient, cart_id: str, product_id: UUID, quantity: int
) -> tuple[int, dict[str, Any]]:
    response = await client.post(
        f"/carts/{cart_id}/items",
        json={"product_id": str(product_id), "quantity": quantity},
    )
    return response.status_code, response.json()


async def test_invalid_offer_rejected(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    cart = await _create_cart(client)
    status_code, _body = await _add_item(client, cart["id"], catalog["cheap"].id, 1)
    assert status_code == 200
    response = await client.post(f"/carts/{cart['id']}/offers", json={"code": "SAVE10"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Offer is not valid for this cart"


async def test_valid_offer_and_free_delivery(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    cart = await _create_cart(client)
    status_code, added = await _add_item(client, cart["id"], catalog["bundle"].id, 2)
    assert status_code == 200
    assert Decimal(str(added["totals"]["subtotal"])) == Decimal("40.00")
    response = await client.post(f"/carts/{cart['id']}/offers", json={"code": "SAVE10"})
    assert response.status_code == 200
    body = response.json()
    assert body["applied_offer"]["code"] == "SAVE10"
    assert body["applied_offer"]["valid"] is True
    assert Decimal(str(body["totals"]["discount"])) == Decimal("4.00")
    free = await client.post(f"/carts/{cart['id']}/offers", json={"code": "FREEDEL"})
    assert free.status_code == 200
    assert Decimal(str(free.json()["totals"]["delivery_fee"])) == Decimal("0.00")


async def test_address_update_persists(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    del catalog
    cart = await _create_cart(client)
    response = await client.put(f"/carts/{cart['id']}/address", json=ADDRESS)
    assert response.status_code == 200
    fetched = await client.get(f"/carts/{cart['id']}")
    assert fetched.status_code == 200
    address = fetched.json()["address"]
    assert address["line1"] == ADDRESS["line1"]
    assert address["city"] == "Austin"
    assert address["postal_code"] == "78701"


async def test_status_transitions_active_pending_paid_converted(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    cart = await _create_cart(client)
    assert cart["status"] == CartStatus.ACTIVE.value
    await _add_item(client, cart["id"], catalog["bundle"].id, 1)
    await client.put(f"/carts/{cart['id']}/address", json=ADDRESS)
    link = await client.post(f"/carts/{cart['id']}/payment-link")
    assert link.status_code == 200
    order = link.json()["order"]
    assert order["status"] == OrderStatus.PENDING_PAYMENT.value
    assert link.json()["payment_url"].startswith("https://payments.example.test/orders/")
    paid = await client.post(f"/test/orders/{order['id']}/mark-paid")
    assert paid.status_code == 200
    assert paid.json()["status"] == OrderStatus.PAID.value
    snapshot = await client.get(f"/test/carts/{cart['id']}/snapshot")
    assert snapshot.status_code == 200
    body = snapshot.json()
    assert body["cart"]["status"] == CartStatus.CONVERTED.value
    assert body["orders"][0]["status"] == OrderStatus.PAID.value
    blocked = await client.post(
        f"/carts/{cart['id']}/items",
        json={"product_id": str(catalog["cheap"].id), "quantity": 1},
    )
    assert blocked.status_code == 409


async def test_abandon_cancels_pending_order(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    cart = await _create_cart(client)
    await _add_item(client, cart["id"], catalog["cheap"].id, 1)
    await client.put(f"/carts/{cart['id']}/address", json=ADDRESS)
    link = await client.post(f"/carts/{cart['id']}/payment-link")
    order_id = link.json()["order"]["id"]
    abandoned = await client.post(f"/test/carts/{cart['id']}/abandon")
    assert abandoned.status_code == 200
    body = abandoned.json()
    assert body["cart"]["status"] == CartStatus.ABANDONED.value
    assert body["orders"][0]["id"] == order_id
    assert body["orders"][0]["status"] == OrderStatus.CANCELLED.value


async def test_stock_and_price_integrity(
    client: AsyncClient, catalog: dict[str, Any], session: AsyncSession
) -> None:
    product: Product = catalog["cheap"]
    assert product.stock == 10
    cart = await _create_cart(client)
    status_code, added = await _add_item(client, cart["id"], product.id, 4)
    assert status_code == 200
    assert Decimal(str(added["items"][0]["unit_price"])) == Decimal("3.00")
    await session.refresh(product)
    assert product.stock == 6
    product.price = Decimal("9.99")
    await session.commit()
    fetched = await client.get(f"/carts/{cart['id']}")
    assert Decimal(str(fetched.json()["items"][0]["unit_price"])) == Decimal("3.00")
    over_status, _over = await _add_item(client, cart["id"], product.id, 7)
    assert over_status == 400
    await client.post(f"/test/carts/{cart['id']}/abandon")
    await session.refresh(product)
    assert product.stock == 10


async def test_paid_order_does_not_restore_stock(
    client: AsyncClient, catalog: dict[str, Any], session: AsyncSession
) -> None:
    product: Product = catalog["bundle"]
    cart = await _create_cart(client)
    await _add_item(client, cart["id"], product.id, 2)
    await session.refresh(product)
    assert product.stock == 6
    await client.put(f"/carts/{cart['id']}/address", json=ADDRESS)
    link = await client.post(f"/carts/{cart['id']}/payment-link")
    await client.post(f"/test/orders/{link.json()['order']['id']}/mark-paid")
    await session.refresh(product)
    assert product.stock == 6


async def test_abandonment_fires_exactly_once(
    client: AsyncClient, catalog: dict[str, Any]
) -> None:
    cart = await _create_cart(client)
    await _add_item(client, cart["id"], catalog["cheap"].id, 1)
    first = await client.post(f"/test/carts/{cart['id']}/abandon")
    second = await client.post(f"/test/carts/{cart['id']}/abandon")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cart"]["abandonment_emitted_at"] is not None
    assert second.json()["cart"]["abandonment_emitted_at"] is not None
    redis = get_redis()
    entries = await redis.xrange(get_settings().redis_abandonment_stream)
    matching = [row for row in entries if row[1]["cart_id"] == cart["id"]]
    assert len(matching) == 1


async def test_sweeper_abandons_idle_cart_once(
    client: AsyncClient,
    catalog: dict[str, Any],
    session: AsyncSession,
) -> None:
    cart = await _create_cart(client)
    await _add_item(client, cart["id"], catalog["cheap"].id, 1)
    db_cart = await session.get(Cart, UUID(cart["id"]))
    assert db_cart is not None
    db_cart.last_activity_at = datetime.now(UTC) - timedelta(seconds=10_000)
    await session.commit()
    emitted = await sweep_abandoned_carts(session, get_redis())
    await session.commit()
    assert emitted >= 1
    again = await sweep_abandoned_carts(session, get_redis())
    await session.commit()
    assert again == 0
    snapshot = await client.get(f"/test/carts/{cart['id']}/snapshot")
    assert snapshot.json()["cart"]["status"] == CartStatus.ABANDONED.value
    entries = await get_redis().xrange(get_settings().redis_abandonment_stream)
    matching = [row for row in entries if row[1]["cart_id"] == cart["id"]]
    assert len(matching) == 1
