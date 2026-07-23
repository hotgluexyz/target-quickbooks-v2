from typing import ClassVar, Optional

from pydantic import BaseModel


class TimeActivity(BaseModel):
    """Local TimeActivity schema until it is added to hotglue-models-accounting."""

    schema_name: ClassVar[str] = "TimeActivities"

    id: Optional[str] = None
    externalId: Optional[str] = None
    transactionDate: Optional[str] = None
    nameOf: Optional[str] = None
    employeeId: Optional[str] = None
    employeeName: Optional[str] = None
    vendorId: Optional[str] = None
    vendorName: Optional[str] = None
    itemId: Optional[str] = None
    itemName: Optional[str] = None
    itemNumber: Optional[str] = None
    payrollItemId: Optional[str] = None
    payrollItemName: Optional[str] = None
    customerId: Optional[str] = None
    customerName: Optional[str] = None
    classId: Optional[str] = None
    className: Optional[str] = None
    billableStatus: Optional[str] = None
    hours: Optional[int] = None
    minutes: Optional[int] = None
    hourlyRate: Optional[float] = None
    taxable: Optional[bool] = None
    description: Optional[str] = None
