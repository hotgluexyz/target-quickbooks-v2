from typing import Dict, List

from hotglue_models_accounting.accounting import PaymentTerm
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.payment_term_schema_mapper import PaymentTermSchemaMapper


class PaymentTermSink(QuickbooksBatchSink):
    name = "paymentTerms"
    record_type = "Term"
    unified_schema = PaymentTerm
    auto_validate_unified_schema = False

    def get_batch_reference_data(self, records: List) -> Dict:
        terms = []
        term_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        term_names = {
            record["name"].replace("'", r"\'") for record in records if record.get("name")
        }

        select_statement = (
            "Id, Name, SyncToken, Active, DueDays, DiscountPercent, DiscountDays, "
            "DayOfMonthDue, DueNextMonthDays, DiscountDayOfMonth"
        )

        if term_ids:
            term_ids_str = ",".join(term_ids)
            terms += self.quickbooks_client.get_entities(
                "Term",
                select_statement=select_statement,
                where_filter=f"Id in ({term_ids_str}) AND Active IN (true, false)",
            )

        if term_names:
            term_names = {f"'{term_name}'" for term_name in term_names}
            term_names_str = ",".join(term_names)
            terms += self.quickbooks_client.get_entities(
                "Term",
                select_statement=select_statement,
                where_filter=f"Name in ({term_names_str}) AND Active IN (true, false)",
            )

        deduped_terms = {}
        for term in self._target.reference_data.get(self.name, []):
            term_id = term.get("Id")
            if term_id is None:
                continue
            deduped_terms[term_id] = term
        for term in terms:
            term_id = term.get("Id")
            if term_id is None:
                continue
            deduped_terms[term_id] = term

        return {**self._target.reference_data, self.name: list(deduped_terms.values())}

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = PaymentTermSchemaMapper(
            record, self.name, reference_data=reference_data
        ).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {
            "bId": f"{index}",
            "operation": operation_type,
            self.record_type: mapped_record,
        }
