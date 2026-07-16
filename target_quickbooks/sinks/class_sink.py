from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.class_schema_mapper import ClassSchemaMapper


class ClassSink(QuickbooksBatchSink):
    name = "Classes"
    record_type = "Class"
    # unified_schema will be added when hotglue-models-accounting has Class model
    auto_validate_unified_schema = False

    def get_batch_reference_data(self, records: List) -> Dict:
        """
        Get existing classes and parent classes by id or name.
        Note: Classes are returned as a list, but FQN should be used for uniqueness.
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
                where_filter=f"Id in ({class_ids_str})"
            )
        
        if class_names:
            class_names = {f"'{class_name}'" for class_name in class_names}
            class_names_str = ",".join(class_names)
            classes += self.quickbooks_client.get_entities(
                "Class", 
                select_statement="Id, Name, FullyQualifiedName, SyncToken, Active", 
                where_filter=f"Name in ({class_names_str})"
            )

        return {**self._target.reference_data, self.name: classes}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = ClassSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
