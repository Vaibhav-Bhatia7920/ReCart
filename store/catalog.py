from decimal import Decimal
from typing import TypedDict

from store.models import OfferKind


class ProductSeed(TypedDict):
    name: str
    category: str
    price: str
    stock: int
    rx: bool


class OfferSeed(TypedDict):
    code: str
    kind: OfferKind
    percent_off: Decimal | None
    min_subtotal: Decimal


# Names and categories only. No dosage, descriptions, or health claims.
PRODUCTS: list[ProductSeed] = [
    {"name": "Pain Relief Tablets", "category": "Pain", "price": "6.49", "stock": 40, "rx": False},
    {"name": "Pain Relief Cream", "category": "Pain", "price": "8.99", "stock": 25, "rx": False},
    {"name": "Heat Patches", "category": "Pain", "price": "7.25", "stock": 30, "rx": False},
    {"name": "Allergy Tablets", "category": "Allergy", "price": "9.50", "stock": 35, "rx": False},
    {"name": "Allergy Nasal Spray", "category": "Allergy", "price": "12.00", "stock": 20, "rx": False},
    {"name": "Eye Drops", "category": "Allergy", "price": "5.75", "stock": 28, "rx": False},
    {"name": "Cough Syrup", "category": "Cold", "price": "7.80", "stock": 22, "rx": False},
    {"name": "Cough Drops", "category": "Cold", "price": "3.25", "stock": 50, "rx": False},
    {"name": "Cold Tablets", "category": "Cold", "price": "8.40", "stock": 32, "rx": False},
    {"name": "Throat Lozenges", "category": "Cold", "price": "4.10", "stock": 45, "rx": False},
    {"name": "Antacid Chews", "category": "Digestion", "price": "5.20", "stock": 36, "rx": False},
    {"name": "Stomach Tablets", "category": "Digestion", "price": "6.90", "stock": 24, "rx": False},
    {"name": "Fiber Gummies", "category": "Digestion", "price": "11.50", "stock": 18, "rx": False},
    {"name": "Probiotic Capsules", "category": "Digestion", "price": "14.25", "stock": 16, "rx": False},
    {"name": "Adhesive Bandages", "category": "First Aid", "price": "3.99", "stock": 60, "rx": False},
    {"name": "Gauze Pads", "category": "First Aid", "price": "4.50", "stock": 40, "rx": False},
    {"name": "Antiseptic Wipes", "category": "First Aid", "price": "4.75", "stock": 38, "rx": False},
    {"name": "Instant Cold Pack", "category": "First Aid", "price": "2.99", "stock": 25, "rx": False},
    {"name": "Lip Balm", "category": "Personal Care", "price": "2.50", "stock": 55, "rx": False},
    {"name": "Hand Sanitizer", "category": "Personal Care", "price": "3.75", "stock": 48, "rx": False},
    {"name": "Face Tissues", "category": "Personal Care", "price": "2.20", "stock": 70, "rx": False},
    {"name": "Cotton Swabs", "category": "Personal Care", "price": "3.10", "stock": 42, "rx": False},
    {"name": "Sleep Tablets", "category": "Sleep", "price": "9.99", "stock": 20, "rx": False},
    {"name": "Sleep Gummies", "category": "Sleep", "price": "10.50", "stock": 18, "rx": False},
    {"name": "Vitamin C Gummies", "category": "Vitamins", "price": "8.25", "stock": 26, "rx": False},
    {"name": "Vitamin D Tablets", "category": "Vitamins", "price": "7.60", "stock": 30, "rx": False},
    {"name": "Multivitamin Tablets", "category": "Vitamins", "price": "12.75", "stock": 22, "rx": False},
    {"name": "Calcium Tablets", "category": "Vitamins", "price": "9.20", "stock": 19, "rx": False},
    {"name": "Moisturizing Lotion", "category": "Skin", "price": "6.15", "stock": 27, "rx": False},
    {"name": "Sunscreen Lotion", "category": "Skin", "price": "11.00", "stock": 21, "rx": False},
    {"name": "Hydrocortisone Cream", "category": "Skin", "price": "5.40", "stock": 17, "rx": False},
    {"name": "Inhaler", "category": "Respiratory", "price": "29.00", "stock": 8, "rx": True},
    {"name": "Insulin Vials", "category": "Diabetes", "price": "42.00", "stock": 6, "rx": True},
    {"name": "Thyroid Tablets", "category": "Hormones", "price": "18.50", "stock": 10, "rx": True},
    {"name": "Blood Pressure Tablets", "category": "Heart", "price": "16.75", "stock": 12, "rx": True},
    {"name": "Antibiotic Ointment", "category": "First Aid", "price": "13.25", "stock": 9, "rx": True},
]

OFFERS: list[OfferSeed] = [
    {
        "code": "SAVE10",
        "kind": OfferKind.PERCENT_OFF_OVER_THRESHOLD,
        "percent_off": Decimal("10.00"),
        "min_subtotal": Decimal("25.00"),
    },
    {
        "code": "SAVE20",
        "kind": OfferKind.PERCENT_OFF_OVER_THRESHOLD,
        "percent_off": Decimal("20.00"),
        "min_subtotal": Decimal("50.00"),
    },
    {
        "code": "FREEDEL",
        "kind": OfferKind.FREE_DELIVERY,
        "percent_off": None,
        "min_subtotal": Decimal("15.00"),
    },
]
