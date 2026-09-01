from unittest.mock import MagicMock

from target_quickbooks.target import TargetQuickBooks


class TestTargetReferenceData:
    def test_payment_terms_loaded_once_including_inactive(self):
        all_terms = [
            {"Id": "3", "Name": "Net 30", "Active": True},
            {"Id": "4", "Name": "Old Term", "Active": False},
        ]

        target = MagicMock()
        target.quickbooks_client.get_entities.side_effect = lambda entity_type, **kwargs: (
            all_terms if entity_type == "Term" else []
        )

        reference_data = TargetQuickBooks.get_reference_data(target)

        term_calls = [
            call
            for call in target.quickbooks_client.get_entities.call_args_list
            if call.args and call.args[0] == "Term"
        ]
        assert len(term_calls) == 1
        assert term_calls[0].kwargs == {"where_filter": "Active IN (true, false)"}
        assert reference_data["paymentTerms"] == all_terms
