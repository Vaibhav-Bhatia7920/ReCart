import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from store.models import CartStatus, OfferKind, OrderStatus


class AddItemRequest(BaseModel):
    product_id: uuid.UUID
    quantity: int = Field(ge=1)


class ApplyOfferRequest(BaseModel):
    code: str = Field(min_length=1, max_length=40)


class AddressWrite(BaseModel):
    line1: str = Field(min_length=1, max_length=200)
    line2: str | None = Field(default=None, max_length=200)
    city: str = Field(min_length=1, max_length=100)
    region: str = Field(min_length=1, max_length=100)
    postal_code: str = Field(min_length=1, max_length=20)
    country: str = Field(min_length=1, max_length=80)


class ProductRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    category: str
    price: Decimal
    stock: int
    requires_prescription: bool


class OfferRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    code: str
    kind: OfferKind
    percent_off: Decimal | None
    min_subtotal: Decimal
    valid: bool


class CartItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    quantity: int
    unit_price: Decimal
    line_total: Decimal


class AddressRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    line1: str
    line2: str | None
    city: str
    region: str
    postal_code: str
    country: str


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    cart_id: uuid.UUID
    status: OrderStatus
    payment_url: str
    subtotal: Decimal
    discount: Decimal
    delivery_fee: Decimal
    total: Decimal
    created_at: datetime
    paid_at: datetime | None


class TotalsRead(BaseModel):
    subtotal: Decimal
    discount: Decimal
    delivery_fee: Decimal
    total: Decimal


class CartRead(BaseModel):
    id: uuid.UUID
    status: CartStatus
    items: list[CartItemRead]
    address: AddressRead | None
    applied_offer: OfferRead | None
    totals: TotalsRead
    last_activity_at: datetime
    abandonment_emitted_at: datetime | None


class PaymentLinkRead(BaseModel):
    order: OrderRead
    payment_url: str


class StoreSnapshot(BaseModel):
    cart: CartRead
    orders: list[OrderRead]
