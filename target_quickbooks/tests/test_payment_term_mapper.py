import pytest

from target_quickbooks.mapper import payment_term_from_unified


class TestPaymentTermMapper:
    def test_maps_standard_term(self):
        payload = payment_term_from_unified(
            {
                "name": "Net 30",
                "paymentType": "STANDARD",
                "dueDays": 30,
                "discountPercent": 2,
                "discountDays": 10,
                "active": True,
            }
        )

        assert payload == {
            "Name": "Net 30",
            "Active": True,
            "DiscountPercent": 2,
            "DiscountDays": 10,
            "DueDays": 30,
        }

    def test_maps_date_driven_term(self):
        payload = payment_term_from_unified(
            {
                "name": "HG Test Date Driven",
                "paymentType": "DATE_DRIVEN",
                "dayOfMonthDue": 15,
                "dueNextMonthDays": 5,
                "discountDayOfMonth": 10,
                "discountPercent": 1,
                "active": True,
            }
        )

        assert payload == {
            "Name": "HG Test Date Driven",
            "Active": True,
            "DiscountPercent": 1,
            "DayOfMonthDue": 15,
            "DueNextMonthDays": 5,
            "DiscountDayOfMonth": 10,
        }

    def test_omits_null_fields(self):
        payload = payment_term_from_unified({"name": "Net 15", "dueDays": 15})

        assert payload == {"Name": "Net 15", "DueDays": 15}

    def test_raises_when_name_missing(self):
        with pytest.raises(ValueError, match="name is required"):
            payment_term_from_unified({"dueDays": 30})
