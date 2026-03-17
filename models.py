"""
models.py - SQLAlchemy ORM models for core inventory tables.

These map to the tables created by load_data.py (from the Excel sheets)
plus new tables for the reservation and waitlist systems.
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.sql import func
from database import Base


# ---------------------------------------------------------------------------
# Tables loaded directly from Excel sheets
# ---------------------------------------------------------------------------

class Store(Base):
    __tablename__ = "Stores"
    # Dataset columns (from retail_dataset.xlsx → load_data.py → SQLite)
    store_id = Column(String, primary_key=True, index=True)
    city = Column(String)
    store_type = Column(String)
    staff_count = Column(Integer)


class Product(Base):
    __tablename__ = "Products"
    # Dataset columns (note: dataset does not include a separate product_name)
    product_id = Column(String, primary_key=True, index=True)
    category = Column(String)
    price_usd = Column(Integer)
    collection_season = Column(String)


class InventoryRecord(Base):
    """Source-of-truth stock from Store_Inventory_Records sheet."""
    __tablename__ = "Store_Inventory_Records"
    store_id = Column(String, primary_key=True, index=True)
    product_id = Column(String, primary_key=True, index=True)
    recorded_stock_level = Column(Integer)
    last_update_time = Column(String)


class WebsiteInventoryView(Base):
    __tablename__ = "Website_Inventory_View"
    store_id    = Column(String, primary_key=True)
    product_id  = Column(String, primary_key=True)
    online_stock = Column(Integer)
    last_synced  = Column(String)


class OnlineOrder(Base):
    __tablename__ = "Online_Orders"
    order_id         = Column(String, primary_key=True, index=True)
    order_time       = Column(String)
    product_id       = Column(String, index=True)
    fulfillment_store = Column(String, index=True)
    quantity         = Column(Integer)
    order_status     = Column(String)


class InventoryAudit(Base):
    __tablename__ = "Inventory_Audits"
    store_id       = Column(String, primary_key=True)
    product_id     = Column(String, primary_key=True)
    recorded_stock = Column(Integer)
    actual_stock   = Column(Integer)
    audit_date     = Column(String)


class StoreTransfer(Base):
    __tablename__ = "Store_Transfers"
    transfer_id       = Column(String, primary_key=True, index=True)
    source_store      = Column(String)
    destination_store = Column(String)
    product_id        = Column(String)
    quantity          = Column(Integer)
    transfer_date     = Column(String)


# ---------------------------------------------------------------------------
# New tables for the real-time system
# ---------------------------------------------------------------------------

class Reservation(Base):
    """
    Represents a stock lock held for a customer for 10 minutes.
    Available stock = actual_stock - SUM(active reservations).
    """
    __tablename__ = "reservations"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String, index=True, nullable=False)
    store_id   = Column(String, index=True, nullable=False)
    quantity   = Column(Integer, nullable=False)
    customer   = Column(String, nullable=True)           # email / session id
    # Status: "active" | "completed" | "expired" | "cancelled"
    status     = Column(String, default="active", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)


class Waitlist(Base):
    """Customers who want to be notified when stock is available."""
    __tablename__ = "waitlist"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    product_id         = Column(String, index=True, nullable=False)
    store_id           = Column(String, index=True, nullable=False)
    customer_email     = Column(String, nullable=False)
    requested_quantity = Column(Integer, default=1)
    created_at         = Column(DateTime(timezone=True), server_default=func.now())
