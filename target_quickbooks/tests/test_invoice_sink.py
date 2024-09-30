def test_process_record_create_invoice(mock_invoice_sink, mock_invoice_dict):
    record = mock_invoice_dict
    record.pop("id")  # for create an invoice, it should not have an id

    # context need to exist before the process record, so it could be updated
    context = {}

    mock_invoice_sink.process_record(record, context)

    assert len(context["records"]) == 1
    assert context["records"][0][0] == "Invoice"
    assert context["records"][0][2] == "create"

def test_process_record_update_invoice(mock_invoice_sink, mock_invoice_dict):
    record = mock_invoice_dict

    # context need to exist before the process record, so it could be updated
    context = {}

    invoice_details = {"1": {"SyncToken": "token"}}

    # Call process_record
    mock_invoice_sink.get_entities.return_value = invoice_details
    mock_invoice_sink.process_record(record, context)

    # Check if the invoice was created correctly
    assert len(context["records"]) == 1
    assert context["records"][0][0] == "Invoice"
    assert context["records"][0][2] == "update"
    assert context["records"][0][1]["SyncToken"] == "token"

def test_process_record_invoice_not_found(mock_invoice_sink, mock_invoice_dict, capsys):
    record = mock_invoice_dict
    record["id"] = "2"

    context = {}

    mock_invoice_sink.get_entities.return_value = {}
    mock_invoice_sink.process_record(record, context)

    captured = capsys.readouterr()
    assert f"Invoice {record['id']} not found. Skipping..." in captured.out
    assert len(context.get("records", [])) == 0
