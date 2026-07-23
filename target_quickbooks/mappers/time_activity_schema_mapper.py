from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, InvalidInputError, RecordNotFound


class TimeActivitySchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
    ]

    field_mappings = {
        "externalId": "externalId",
        "transactionDate": "TxnDate",
        "billableStatus": "BillableStatus",
        "hours": "Hours",
        "minutes": "Minutes",
        "hourlyRate": "HourlyRate",
        "taxable": "Taxable",
        "description": "Description",
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_name_of_and_person(),
            **self._map_item(),
            **self._map_payroll_item(),
            **self._map_customer(),
            **self._map_class(),
        }

        self._map_fields(payload)

        return payload

    def _map_name_of_and_person(self) -> Dict:
        name_of = self.record.get("nameOf")
        has_employee = bool(self.record.get("employeeId") or self.record.get("employeeName"))
        has_vendor = bool(self.record.get("vendorId") or self.record.get("vendorName"))

        if name_of and name_of not in ("Employee", "Vendor"):
            raise InvalidInputError(f"Invalid nameOf={name_of}. Expected 'Employee' or 'Vendor'")

        if not name_of:
            if has_employee and has_vendor:
                raise InvalidInputError(
                    "Provide nameOf when both employee and vendor references are present"
                )
            if has_employee:
                name_of = "Employee"
            elif has_vendor:
                name_of = "Vendor"
            else:
                raise InvalidInputError(
                    "TimeActivity requires an employee (employeeId/employeeName) or "
                    "vendor (vendorId/vendorName) reference"
                )

        if name_of == "Employee":
            if not has_employee:
                raise InvalidInputError(
                    "nameOf=Employee requires employeeId or employeeName"
                )
            return {"NameOf": "Employee", **self._map_employee()}

        if not has_vendor:
            raise InvalidInputError("nameOf=Vendor requires vendorId or vendorName")

        return {"NameOf": "Vendor", **self._map_vendor()}

    def _map_employee(self) -> Dict:
        found_employee = None

        if employee_id := self.record.get("employeeId"):
            found_employee = next(
                (
                    employee
                    for employee in self.reference_data.get("Employees", [])
                    if employee["Id"] == employee_id
                ),
                None,
            )

        if (employee_name := self.record.get("employeeName")) and found_employee is None:
            found_employee = next(
                (
                    employee
                    for employee in self.reference_data.get("Employees", [])
                    if employee["DisplayName"] == employee_name
                ),
                None,
            )

        if (self.record.get("employeeId") or self.record.get("employeeName")) and found_employee is None:
            raise RecordNotFound(
                f"Employee could not be found in QBO with Id={self.record.get('employeeId')} / "
                f"Name={self.record.get('employeeName')}"
            )

        if found_employee:
            return {
                "EmployeeRef": {
                    "value": found_employee["Id"],
                    "name": found_employee["DisplayName"],
                }
            }

        return {}

    def _map_item(self) -> Dict:
        found_item = None
        item_id = self.record.get("itemId")
        item_name = self.record.get("itemName")
        item_sku = self.record.get("itemNumber")

        if item_sku:
            found_item = next(
                (
                    item
                    for item in self.reference_data.get("Items", [])
                    if item.get("Sku") == item_sku
                ),
                None,
            )
            if not found_item:
                raise RecordNotFound(f"An item with Sku={item_sku} could not be found in QBO")

        if item_id and not found_item:
            found_item = next(
                (
                    item
                    for item in self.reference_data.get("Items", [])
                    if item["Id"] == item_id
                ),
                None,
            )

        if item_name and not found_item:
            found_item = next(
                (
                    item
                    for item in self.reference_data.get("Items", [])
                    if item["Name"] == item_name
                ),
                None,
            )

        if (item_id or item_name or item_sku) and found_item is None:
            raise RecordNotFound(
                f"An item with Id={item_id} / Name={item_name} could not be found in QBO"
            )

        if found_item:
            return {
                "ItemRef": {
                    "value": found_item["Id"],
                    "name": found_item["Name"],
                }
            }

        return {}

    def _map_payroll_item(self) -> Dict:
        if not (payroll_item_id := self.record.get("payrollItemId")):
            return {}

        payroll_item_ref = {"value": payroll_item_id}
        if payroll_item_name := self.record.get("payrollItemName"):
            payroll_item_ref["name"] = payroll_item_name

        return {"PayrollItemRef": payroll_item_ref}
