from unittest.mock import MagicMock, patch

from target_quickbooks.client import QuickbooksSink


class TestClientReferenceData:
    def test_terms_loaded_once_and_active_subset_derived(self, mock_target):
        all_terms = {
            "Net 30": {"Id": "3", "Name": "Net 30", "Active": True},
            "Old Term": {"Id": "4", "Name": "Old Term", "Active": False},
        }

        with patch.object(QuickbooksSink, "is_token_valid", return_value=True):
            with patch.object(QuickbooksSink, "get_reference_data"):
                sink = QuickbooksSink(
                    target=mock_target,
                    stream_name="PaymentTerms",
                    schema={"properties": {}},
                    key_properties=None,
                )

        def get_entities_side_effect(entity_type, **kwargs):
            if entity_type == "Term":
                return all_terms
            return {}

        sink.get_entities = MagicMock(side_effect=get_entities_side_effect)
        sink.get_reference_data()

        term_calls = [
            call
            for call in sink.get_entities.call_args_list
            if call.args and call.args[0] == "Term"
        ]
        assert len(term_calls) == 1
        assert term_calls[0].kwargs == {"key": "Name", "check_active": False}
        assert sink.all_terms_by_name == all_terms
        assert sink.terms == {"Net 30": all_terms["Net 30"]}
        assert sink.sales_terms == sink.terms
