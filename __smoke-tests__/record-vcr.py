import json

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetQuickBooksV2TestRunner(VCRTargetTestRunner):
    def module(self) -> str:
        return "target_quickbooks"

    def launch(self):
        from target_quickbooks.target import TargetQuickBooks

        TargetQuickBooks.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        my_vcr = vcr.VCR()
        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=["authorization"],
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
        )


if __name__ == "__main__":
    TargetQuickBooksV2TestRunner.main()
