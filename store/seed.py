import asyncio
from decimal import Decimal

from sqlalchemy import select

from app.db import close_db, get_session_factory, init_engine
from app.settings import get_settings
from store.catalog import OFFERS, PRODUCTS
from store.models import Offer, Product


async def seed_catalog() -> None:
    factory = get_session_factory()
    async with factory() as session:
        existing = await session.execute(select(Product.id).limit(1))
        if existing.scalar_one_or_none() is None:
            for product in PRODUCTS:
                session.add(
                    Product(
                        name=product["name"],
                        category=product["category"],
                        price=Decimal(product["price"]),
                        stock=product["stock"],
                        requires_prescription=product["rx"],
                    )
                )
        existing_offers = await session.execute(select(Offer.id).limit(1))
        if existing_offers.scalar_one_or_none() is None:
            for offer in OFFERS:
                session.add(
                    Offer(
                        code=offer["code"],
                        kind=offer["kind"],
                        percent_off=offer["percent_off"],
                        min_subtotal=offer["min_subtotal"],
                    )
                )
        await session.commit()


async def _run() -> None:
    settings = get_settings()
    init_engine(settings.database_url)
    try:
        await seed_catalog()
    finally:
        await close_db()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
