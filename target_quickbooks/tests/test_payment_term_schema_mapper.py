import pytest

from target_quickbooks.mappers.base_mapper import InvalidInputError, RecordNotFound
from target_quickbooks.mappers.payment_term_schema_mapper import PaymentTermSchemaMapper


class TestPaymentTermSchemaMapper:
    def test_maps_standard_term(self):
        payload = PaymentTermSchemaMapper(
            {
                "name": "Net 30",
                "paymentType": "STANDARD",
                "dueDays": 30,
                "discountPercent": 2,
                "discountDays": 10,
                "active": True,
            },
            "paymentTerms",
            reference_data={"paymentTerms": []},
        ).to_quickbooks()

        assert payload == {
            "Name": "Net 30",
            "Active": True,
            "DiscountPercent": 2,
            "DiscountDays": 10,
            "DueDays": 30,
        }

    def test_maps_date_driven_term(self):
        payload = PaymentTermSchemaMapper(
            {
                "name": "HG Test Date Driven",
                "paymentType": "DATE_DRIVEN",
                "dayOfMonthDue": 15,
                "dueNextMonthDays": 5,
                "discountDayOfMonth": 10,
                "discountPercent": 1,
                "active": True,
            },
            "paymentTerms",
            reference_data={"paymentTerms": []},
        ).to_quickbooks()

        assert payload == {
            "Name": "HG Test Date Driven",
            "Active": True,
            "DiscountPercent": 1,
            "DayOfMonthDue": 15,
            "DueNextMonthDays": 5,
            "DiscountDayOfMonth": 10,
        }

    def test_omits_null_fields(self):
        payload = PaymentTermSchemaMapper(
            {"name": "Net 15", "dueDays": 15},
            "paymentTerms",
            reference_data={"paymentTerms": []},
        ).to_quickbooks()

        assert payload == {"Name": "Net 15", "DueDays": 15}

    def test_raises_when_name_missing(self):
        with pytest.raises(InvalidInputError, match="name is required"):
            PaymentTermSchemaMapper(
                {"dueDays": 30},
                "paymentTerms",
                reference_data={"paymentTerms": []},
            ).to_quickbooks()

    def test_updates_existing_payment_term_by_name(self):
        payload = PaymentTermSchemaMapper(
            {"name": "Net 30", "dueDays": 30},
            "paymentTerms",
            reference_data={
                "paymentTerms": [
                    {"Id": "3", "SyncToken": "1", "Name": "Net 30", "Active": True}
                ]
            },
        ).to_quickbooks()

        assert payload["Id"] == "3"
        assert payload["SyncToken"] == "1"
        assert payload["sparse"] is True
        assert payload["DueDays"] == 30

    def test_updates_existing_payment_term_by_id(self):
        payload = PaymentTermSchemaMapper(
            {"id": "3", "name": "Net 30", "dueDays": 30},
            "paymentTerms",
            reference_data={
                "paymentTerms": [
                    {"Id": "3", "SyncToken": "2", "Name": "Net 30", "Active": True}
                ]
            },
        ).to_quickbooks()

        assert payload["Id"] == "3"
        assert payload["SyncToken"] == "2"

    def test_raises_when_id_not_found(self):
        with pytest.raises(RecordNotFound, match="Payment term 999 not found"):
            PaymentTermSchemaMapper(
                {"id": "999", "name": "Net 30", "dueDays": 30},
                "paymentTerms",
                reference_data={"paymentTerms": []},
            ).to_quickbooks()
