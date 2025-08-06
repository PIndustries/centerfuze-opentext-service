"""
OpenText models for the CenterFuze OpenText Service.

This module defines the data models for OpenText account management,
fax usage tracking, number porting, and usage data aggregation.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
import json


class AccountStatus(Enum):
    """OpenText account status enumeration."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    CANCELLED = "cancelled"


class PortingStatus(Enum):
    """Number porting status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class UsageDataType(Enum):
    """Usage data type enumeration."""
    FAX_PAGES_SENT = "fax_pages_sent"
    FAX_PAGES_RECEIVED = "fax_pages_received"
    PHONE_MINUTES = "phone_minutes"
    SMS_MESSAGES = "sms_messages"
    DATA_TRANSFER = "data_transfer"


@dataclass
class OpenTextAccount:
    """
    OpenText account model representing customer account information.
    
    Attributes:
        account_id: Unique identifier for the account
        account_name: Display name for the account
        child_accounts: List of child account IDs
        status: Current account status
        created_date: Account creation timestamp
        last_updated: Last modification timestamp
        contact_info: Account contact information
        settings: Account-specific settings and configurations
        billing_info: Billing and payment information
    """
    account_id: str
    account_name: str
    child_accounts: List[str] = field(default_factory=list)
    status: AccountStatus = AccountStatus.ACTIVE
    created_date: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    contact_info: Dict[str, Any] = field(default_factory=dict)
    settings: Dict[str, Any] = field(default_factory=dict)
    billing_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the account to a dictionary representation."""
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "child_accounts": self.child_accounts,
            "status": self.status.value,
            "created_date": self.created_date.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "contact_info": self.contact_info,
            "settings": self.settings,
            "billing_info": self.billing_info
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenTextAccount':
        """Create an account instance from a dictionary."""
        return cls(
            account_id=data["account_id"],
            account_name=data["account_name"],
            child_accounts=data.get("child_accounts", []),
            status=AccountStatus(data.get("status", AccountStatus.ACTIVE.value)),
            created_date=datetime.fromisoformat(data.get("created_date", datetime.now().isoformat())),
            last_updated=datetime.fromisoformat(data.get("last_updated", datetime.now().isoformat())),
            contact_info=data.get("contact_info", {}),
            settings=data.get("settings", {}),
            billing_info=data.get("billing_info", {})
        )

    def add_child_account(self, child_account_id: str) -> None:
        """Add a child account to this account."""
        if child_account_id not in self.child_accounts:
            self.child_accounts.append(child_account_id)
            self.last_updated = datetime.now()

    def remove_child_account(self, child_account_id: str) -> None:
        """Remove a child account from this account."""
        if child_account_id in self.child_accounts:
            self.child_accounts.remove(child_account_id)
            self.last_updated = datetime.now()


@dataclass
class FaxUsage:
    """
    Fax usage tracking model for OpenText fax services.
    
    Attributes:
        account_id: Account identifier
        pages_sent: Number of fax pages sent
        pages_received: Number of fax pages received
        period_start: Start of the reporting period
        period_end: End of the reporting period
        cost_per_page: Cost per fax page
        total_cost: Total cost for the period
        usage_details: Detailed usage breakdown
    """
    account_id: str
    pages_sent: int = 0
    pages_received: int = 0
    period_start: datetime = field(default_factory=datetime.now)
    period_end: datetime = field(default_factory=datetime.now)
    cost_per_page: float = 0.0
    total_cost: float = 0.0
    usage_details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the fax usage to a dictionary representation."""
        return {
            "account_id": self.account_id,
            "pages_sent": self.pages_sent,
            "pages_received": self.pages_received,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "cost_per_page": self.cost_per_page,
            "total_cost": self.total_cost,
            "usage_details": self.usage_details
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FaxUsage':
        """Create a fax usage instance from a dictionary."""
        return cls(
            account_id=data["account_id"],
            pages_sent=data.get("pages_sent", 0),
            pages_received=data.get("pages_received", 0),
            period_start=datetime.fromisoformat(data.get("period_start", datetime.now().isoformat())),
            period_end=datetime.fromisoformat(data.get("period_end", datetime.now().isoformat())),
            cost_per_page=data.get("cost_per_page", 0.0),
            total_cost=data.get("total_cost", 0.0),
            usage_details=data.get("usage_details", {})
        )

    def calculate_total_pages(self) -> int:
        """Calculate total pages (sent + received)."""
        return self.pages_sent + self.pages_received

    def update_cost(self) -> None:
        """Update total cost based on pages and cost per page."""
        total_pages = self.calculate_total_pages()
        self.total_cost = total_pages * self.cost_per_page


@dataclass
class NumberPorting:
    """
    Number porting model for managing phone number transfers.
    
    Attributes:
        phone_number: The phone number being ported
        status: Current porting status
        carrier: Current or previous carrier
        port_date: Scheduled or completed port date
        account_id: Associated account identifier
        request_date: Date the port request was made
        completion_date: Date the port was completed (if applicable)
        notes: Additional notes or comments
        documents: Associated document references
    """
    phone_number: str
    status: PortingStatus
    carrier: str
    account_id: str
    port_date: Optional[datetime] = None
    request_date: datetime = field(default_factory=datetime.now)
    completion_date: Optional[datetime] = None
    notes: str = ""
    documents: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the number porting to a dictionary representation."""
        return {
            "phone_number": self.phone_number,
            "status": self.status.value,
            "carrier": self.carrier,
            "account_id": self.account_id,
            "port_date": self.port_date.isoformat() if self.port_date else None,
            "request_date": self.request_date.isoformat(),
            "completion_date": self.completion_date.isoformat() if self.completion_date else None,
            "notes": self.notes,
            "documents": self.documents
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NumberPorting':
        """Create a number porting instance from a dictionary."""
        return cls(
            phone_number=data["phone_number"],
            status=PortingStatus(data["status"]),
            carrier=data["carrier"],
            account_id=data["account_id"],
            port_date=datetime.fromisoformat(data["port_date"]) if data.get("port_date") else None,
            request_date=datetime.fromisoformat(data.get("request_date", datetime.now().isoformat())),
            completion_date=datetime.fromisoformat(data["completion_date"]) if data.get("completion_date") else None,
            notes=data.get("notes", ""),
            documents=data.get("documents", [])
        )

    def complete_port(self) -> None:
        """Mark the port as completed."""
        self.status = PortingStatus.COMPLETED
        self.completion_date = datetime.now()

    def fail_port(self, reason: str) -> None:
        """Mark the port as failed with a reason."""
        self.status = PortingStatus.FAILED
        self.notes += f"\nFailed: {reason}" if self.notes else f"Failed: {reason}"


@dataclass
class UsageData:
    """
    Generic usage data model for various service types.
    
    Attributes:
        account_id: Account identifier
        usage_type: Type of usage being tracked
        quantity: Usage quantity (pages, minutes, messages, etc.)
        period_start: Start of the reporting period
        period_end: End of the reporting period
        cost: Cost associated with this usage
        metadata: Additional usage metadata
        created_at: Timestamp when usage was recorded
    """
    account_id: str
    usage_type: UsageDataType
    quantity: float
    period_start: datetime
    period_end: datetime
    cost: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the usage data to a dictionary representation."""
        return {
            "account_id": self.account_id,
            "usage_type": self.usage_type.value,
            "quantity": self.quantity,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "cost": self.cost,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UsageData':
        """Create a usage data instance from a dictionary."""
        return cls(
            account_id=data["account_id"],
            usage_type=UsageDataType(data["usage_type"]),
            quantity=data["quantity"],
            period_start=datetime.fromisoformat(data["period_start"]),
            period_end=datetime.fromisoformat(data["period_end"]),
            cost=data.get("cost", 0.0),
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat()))
        )

    def calculate_rate(self) -> float:
        """Calculate the rate (cost per unit) for this usage."""
        if self.quantity == 0:
            return 0.0
        return self.cost / self.quantity


@dataclass
class UsageAggregation:
    """
    Usage aggregation model for summarizing usage across periods and accounts.
    
    Attributes:
        account_ids: List of accounts included in aggregation
        usage_type: Type of usage being aggregated
        total_quantity: Total aggregated quantity
        total_cost: Total aggregated cost
        period_start: Start of the aggregation period
        period_end: End of the aggregation period
        breakdown: Breakdown by account or other dimensions
        created_at: Timestamp when aggregation was created
    """
    account_ids: List[str]
    usage_type: UsageDataType
    total_quantity: float
    total_cost: float
    period_start: datetime
    period_end: datetime
    breakdown: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the usage aggregation to a dictionary representation."""
        return {
            "account_ids": self.account_ids,
            "usage_type": self.usage_type.value,
            "total_quantity": self.total_quantity,
            "total_cost": self.total_cost,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "breakdown": self.breakdown,
            "created_at": self.created_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UsageAggregation':
        """Create a usage aggregation instance from a dictionary."""
        return cls(
            account_ids=data["account_ids"],
            usage_type=UsageDataType(data["usage_type"]),
            total_quantity=data["total_quantity"],
            total_cost=data["total_cost"],
            period_start=datetime.fromisoformat(data["period_start"]),
            period_end=datetime.fromisoformat(data["period_end"]),
            breakdown=data.get("breakdown", {}),
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat()))
        )

    def calculate_average_rate(self) -> float:
        """Calculate the average rate across all usage."""
        if self.total_quantity == 0:
            return 0.0
        return self.total_cost / self.total_quantity