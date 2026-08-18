from typing import Dict, List

from hotglue_models_accounting.accounting import Class
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.class_schema_mapper import ClassSchemaMapper


class ClassSink(QuickbooksBatchSink):
    name = "Classes"
    record_type = "Class"
    unified_schema = Class
    auto_validate_unified_schema = True
    buffer_until_stream_end = True

    def get_batch_reference_data(self, records: List) -> Dict:
        """
        Get existing classes and parent classes by id or name.
        Note: Classes are returned as a list, but FQN should be used for uniqueness.

        Freshly fetched classes are layered on top of the preloaded full Classes
        list (deduped by Id, fresh data winning) rather than replacing it. The QBO
        Name field only holds the leaf name, so a parentName that is a
        FullyQualifiedName (e.g. "Dept:Sales") never matches the ``Name in (...)``
        query. Preserving the preloaded list keeps such parents resolvable by FQN.
        """
        classes = []
        class_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        class_ids.update({f"'{record['parentId']}'" for record in records if record.get("parentId")})
        
        # For names, we need to escape single quotes
        class_names = {record['name'].replace("'", r"\'") for record in records if record.get("name")}
        class_names.update({record['parentName'].replace("'", r"\'") for record in records if record.get("parentName")})

        if class_ids:
            class_ids_str = ",".join(class_ids)
            classes += self.quickbooks_client.get_entities(
                "Class", 
                select_statement="Id, Name, FullyQualifiedName, SyncToken, Active", 
                where_filter=f"Id in ({class_ids_str}) AND Active IN (true, false)"
            )
        
        if class_names:
            class_names = {f"'{class_name}'" for class_name in class_names}
            class_names_str = ",".join(class_names)
            classes += self.quickbooks_client.get_entities(
                "Class", 
                select_statement="Id, Name, FullyQualifiedName, SyncToken, Active", 
                where_filter=f"Name in ({class_names_str}) AND Active IN (true, false)"
            )

        # Start from the preloaded full Classes list so parents referenced by
        # FullyQualifiedName stay resolvable, then layer the freshly fetched
        # classes on top (fresh data wins) and deduplicate by Id. Deduping avoids
        # inflating ambiguity checks that count distinct classes by Name.
        deduped_classes = {}
        for cls in self._target.reference_data.get(self.name, []):
            class_id = cls.get("Id")
            if class_id is None:
                continue
            deduped_classes[class_id] = cls
        for cls in classes:
            class_id = cls.get("Id")
            if class_id is None:
                continue
            deduped_classes[class_id] = cls

        return {**self._target.reference_data, self.name: list(deduped_classes.values())}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = ClassSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
