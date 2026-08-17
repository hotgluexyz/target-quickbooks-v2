from typing import Dict, List

from hotglue_models_accounting.accounting import Account
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.account_schema_mapper import AccountSchemaMapper


class AccountSink(QuickbooksBatchSink):
    name = "Accounts"
    record_type = "Account"
    unified_schema = Account
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        """
        Get existing accounts and parent accounts by id or name.
        Note: Accounts are returned as a list, but FQN should be used for uniqueness.

        Freshly fetched accounts are layered on top of the preloaded full Accounts
        list (deduped by Id, fresh data winning) rather than replacing it. The QBO
        Name field only holds the leaf name, so a parentName that is a
        FullyQualifiedName (e.g. "Job Expenses:Job Materials:Decks and Patios")
        never matches the ``Name in (...)`` query. Preserving the preloaded list
        keeps such parents resolvable by FQN.
        """
        accounts = []
        account_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        account_ids.update({f"'{record['parentId']}'" for record in records if record.get("parentId")})

        account_names = {record['name'].replace("'", r"\'") for record in records if record.get("name")}
        account_names.update({record['parentName'].replace("'", r"\'") for record in records if record.get("parentName")})

        select_statement = (
            "Id, Name, FullyQualifiedName, SyncToken, Active, ParentRef, AccountType, AcctNum"
        )

        if account_ids:
            account_ids_str = ",".join(account_ids)
            accounts += self.quickbooks_client.get_entities(
                "Account",
                select_statement=select_statement,
                where_filter=f"Id in ({account_ids_str}) AND Active IN (true, false)"
            )

        if account_names:
            account_names = {f"'{account_name}'" for account_name in account_names}
            account_names_str = ",".join(account_names)
            accounts += self.quickbooks_client.get_entities(
                "Account",
                select_statement=select_statement,
                where_filter=f"Name in ({account_names_str}) AND Active IN (true, false)"
            )

        # Start from the preloaded full Accounts list so parents referenced by
        # FullyQualifiedName stay resolvable, then layer the freshly fetched
        # accounts on top (fresh data wins) and deduplicate by Id.
        deduped_accounts = {}
        for acct in self._target.reference_data.get(self.name, []):
            account_id = acct.get("Id")
            if account_id is None:
                continue
            deduped_accounts[account_id] = acct
        for acct in accounts:
            account_id = acct.get("Id")
            if account_id is None:
                continue
            deduped_accounts[account_id] = acct

        return {**self._target.reference_data, self.name: list(deduped_accounts.values())}

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = AccountSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
