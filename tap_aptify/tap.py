from __future__ import annotations

from singer_sdk import SQLTap, SQLStream, SQLConnector
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_aptify.client import aptifyStream, aptifyConnector


class Tapaptify(SQLTap):
    """MSSQL tap class using pytds exclusively."""

    name = "tap-aptify"
    default_stream_class = aptifyStream
    default_connector_class = aptifyConnector
    _tap_connector: SQLConnector = None

    @property
    def tap_connector(self) -> SQLConnector:
        """Return the connector object."""
        if self._tap_connector is None:
            self._tap_connector = self.default_connector_class(dict(self.config))
        return self._tap_connector
    
    @property
    def catalog_dict(self) -> dict:
        """Return the tap's catalog as a dictionary."""
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.tap_connector
        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())
        self._catalog_dict = result
        return self._catalog_dict

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchemy",
            required=True,
            allowed_values=["mssql"],
            default="mssql"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver to use when connecting to the SQL server",
            required=True,
            allowed_values=["pymssql"],
            default="pymssql"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving the SQL Instance",
            required=True
        ),
        th.Property(
            "port",
            th.StringType,
            description="The port on which SQL awaits connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account granted access to the SQL Server",
            required=True
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            required=True,
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection",
            required=True
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "fast_executemany",
                    th.StringType,
                    description="Fast Executemany Mode: True, False"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Parameters: fast_executemany, future"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                    "driver",
                    th.StringType,
                    description="Driver parameter when using specific drivers (for pyodbc)"
                ),
                th.Property(
                    "TrustServerCertificate",
                    th.StringType,
                    description="Yes or No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, TrustServerCertificate"
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl"
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression option is gzip"
                        )
                    )
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="The directory for batch messages\nexample: file://test/batches"
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="Prefix for batch messages\nexample: test-batch-"
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Turn on Higher Defined (HD) JSON Schema types to assist Targets"
        ),
    ).to_dict()

    def discover_streams(self) -> list[SQLStream]:
        """Return a list of available stream objects."""
        result: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(
                self.default_stream_class(
                    tap=self,
                    catalog_entry=catalog_entry,
                    connector=self.tap_connector
                )
            )
        return result

if __name__ == "__main__":
    Tapaptify.cli()
