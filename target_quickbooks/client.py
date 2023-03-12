from target_hotglue.client import HotglueSink
from singer_sdk.sinks import BatchSink

class QuickbooksSink(HotglueSink):
    endpoint = ""
    base_url = ""

    def validate_input(self, record: dict):
        return True
