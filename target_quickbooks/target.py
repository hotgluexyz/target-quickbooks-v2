"""QuickBooks target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_quickbooks.sinks import (
    QuickBooksSink,
)


class TargetQuickBooks(Target):
    """Sample target for QuickBooks."""
    def __init__(
        self,
        config=None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config)

    name = "target-quickbooks"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",th.StringType,required=True
        ),
        th.Property(
            "client_secret",th.StringType,required=True
        ),
        th.Property(
            "refresh_token",th.StringType,required=True
        ),
        th.Property(
            "access_token",th.StringType,required=True
        ),
        th.Property(
            "redirect_uri",th.StringType,required=True
        ),
        th.Property(
            "realmId",th.StringType,required=True
        ),
        th.Property(
            "is_sanbox",th.BooleanType,required=False
        ),
    ).to_dict()
    default_sink_class = QuickBooksSink

if __name__ == '__main__':
    TargetQuickBooks.cli()    