from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from store.models import Cart, CartItem, Offer, OfferKind

MONEY = Decimal("0.01")


def money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def cart_subtotal(items: list[CartItem]) -> Decimal:
    total = sum((item.unit_price * item.quantity for item in items), Decimal("0.00"))
    return money(Decimal(total))


def offer_is_valid(offer: Offer, subtotal: Decimal) -> bool:
    if subtotal < offer.min_subtotal:
        return False
    if offer.kind is OfferKind.PERCENT_OFF_OVER_THRESHOLD:
        return offer.percent_off is not None and offer.percent_off > 0
    if offer.kind is OfferKind.FREE_DELIVERY:
        return True
    return False


def compute_totals(
    items: list[CartItem],
    offer: Offer | None,
    delivery_fee: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    subtotal = cart_subtotal(items)
    discount = Decimal("0.00")
    fee = money(delivery_fee)
    if offer is not None and offer_is_valid(offer, subtotal):
        if offer.kind is OfferKind.PERCENT_OFF_OVER_THRESHOLD and offer.percent_off is not None:
            discount = money(subtotal * offer.percent_off / Decimal("100"))
        elif offer.kind is OfferKind.FREE_DELIVERY:
            fee = Decimal("0.00")
    total = money(subtotal - discount + fee)
    return subtotal, discount, fee, total


def touch_cart(cart: Cart) -> None:
    cart.last_activity_at = datetime.now(UTC)
