from __future__ import annotations

import gzip
import json
import datetime
from base64 import b64encode
from decimal import Decimal
from uuid import uuid4
from typing import Any, Iterable, Iterator

import pendulum
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.batch import BaseBatcher, lazy_chunked_generator
from sqlalchemy import types


class aptifyConnector(SQLConnector):
    """Connects to the MSSQL SQL source using the pytds driver exclusively."""

    def __init__(
        self,
        config: dict | None = None,
        sqlalchemy_url: str | None = None
    ) -> None:
        """Initialize the connector.

        Args:
            config: Configuration dictionary with connection parameters.
            sqlalchemy_url: Optional SQLAlchemy URL string.
        """
        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Builds SQLAlchemy connection string for Azure MSSQL.

        Args:
            config: A dict with connection parameters.

        Returns:
            SQLAlchemy connection string.
        """
        return (
            f"mssql+pymssql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )

    def get_connection(self, config: dict) -> sqlalchemy.engine.Connection:
        """Get a connection object for the MSSQL database.

        Args:
            config: Connection parameters.

        Returns:
            SQLAlchemy connection object.
        """
        engine = sqlalchemy.create_engine(self.get_sqlalchemy_url(config))
        return engine.connect()

    def to_jsonschema_type(
        self,
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type."""
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_jsonschema_type(from_type)
        else:
            return self.org_to_jsonschema_type(from_type)

    def org_to_jsonschema_type(self, from_type):
        """Maps a SQL type to a JSON Schema type using basic mapping."""
        if isinstance(from_type, str):
            type_mapping = {
                "INTEGER": types.Integer(),
                "NUMBER": types.Numeric(),
                "INT": types.Integer(),
                "VARCHAR": types.String(),
                "BOOL": types.Boolean(),
            }
            sql_type_obj = type_mapping.get(from_type.upper(), None)
            if sql_type_obj is None:
                sql_type_obj = types.String()
        else:
            sql_type_obj = from_type

        return super().to_jsonschema_type(sql_type_obj)


    @staticmethod
    def hd_to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a higher-defined JSON Schema equivalent for the given SQL type.

        Args:
            from_type: The SQL type to convert.

        Returns:
            A JSON Schema type definition.
        """
        if isinstance(from_type, str):
            sql_type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = from_type.__name__
        else:
            raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")

        if sql_type_name in ['CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR']:
            maxLength: int = getattr(from_type, 'length')
            if getattr(from_type, 'length'):
                return {"type": ["string"], "maxLength": maxLength}

        if sql_type_name == 'TIME':
            return {"type": ["string"], "format": "time"}

        if sql_type_name == 'UNIQUEIDENTIFIER':
            return {"type": ["string"], "format": "uuid"}

        if sql_type_name == 'XML':
            return {"type": ["string"], "contentMediaType": "application/xml"}

        if sql_type_name in ['BINARY', 'IMAGE', 'VARBINARY']:
            maxLength: int = getattr(from_type, 'length')
            if getattr(from_type, 'length'):
                return {"type": ["string"], "contentEncoding": "base64", "maxLength": maxLength}
            else:
                return {"type": ["string"], "contentEncoding": "base64"}

        if sql_type_name == 'BIT':
            return {"type": ["boolean"]}

        if sql_type_name == 'TINYINT':
            return {"type": ["integer"], "minimum": 0, "maximum": 255}

        if sql_type_name == 'SMALLINT':
            return {"type": ["integer"], "minimum": -32768, "maximum": 32767}

        if sql_type_name == 'INTEGER':
            return {"type": ["integer"], "minimum": -2147483648, "maximum": 2147483647}

        if sql_type_name == 'BIGINT':
            return {"type": ["integer"], "minimum": -9223372036854775808, "maximum": 9223372036854775807}

        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(from_type, 'precision')
            scale: int = getattr(from_type, 'scale')
            if scale == 0:
                return {"type": ["integer"], "minimum": (-pow(10, precision)) + 1, "maximum": (pow(10, precision)) - 1}
            else:
                maximum_as_number = ""
                minimum_as_number = "-"
                for i in range(precision):
                    if i == (precision - scale):
                        maximum_as_number += '.'
                    maximum_as_number += '9'
                minimum_as_number += maximum_as_number

                maximum_scientific_format = "9." + "9" * scale + f"e+{precision}"
                minimum_scientific_format = "-" + maximum_scientific_format

                if "e+" not in str(float(maximum_as_number)):
                    return {"type": ["number"], "minimum": float(minimum_as_number), "maximum": float(maximum_as_number)}
                else:
                    return {"type": ["number"], "minimum": float(minimum_scientific_format), "maximum": float(maximum_scientific_format)}

        if sql_type_name == "SMALLMONEY":
            return {"type": ["number"], "minimum": -214748.3648, "maximum": 214748.3647}

        if sql_type_name == "MONEY":
            return {"type": ["number"], "minimum": -922337203685477.5808, "maximum": 922337203685477.5807}

        if sql_type_name == "FLOAT":
            return {"type": ["number"], "minimum": -1.79e308, "maximum": 1.79e308}

        if sql_type_name == "REAL":
            return {"type": ["number"], "minimum": -3.40e38, "maximum": 3.40e38}

        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a SQLAlchemy type representation for the given JSON Schema type.

        Args:
            jsonschema_type: The JSON Schema type definition.

        Returns:
            A SQLAlchemy TypeEngine object.
        """
        return SQLConnector.to_sql_type(jsonschema_type)

    @staticmethod
    def get_fully_qualified_name(
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The schema name (optional).
            db_name: The database name (optional).
            delimiter: The delimiter to use between parts.

        Returns:
            A fully qualified name string.
        """
        parts = []
        if table_name:
            parts.append(table_name)
        if not parts:
            raise ValueError("Could not generate fully qualified name: " + (table_name or "(unknown-table-name)"))
        return table_name


class CustomJSONEncoder(json.JSONEncoder):
    """Custom class extends json.JSONEncoder to handle special types."""

    def default(self, obj):
        """Override default serialization for specific types."""
        if isinstance(obj, datetime.datetime):
            return pendulum.instance(obj).isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        if isinstance(obj, datetime.time):
            return obj.isoformat(timespec='seconds')
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class JSONLinesBatcher(BaseBatcher):
    """JSON Lines Record Batcher."""

    encoder_class = CustomJSONEncoder

    def get_batches(
        self,
        records: Iterator[dict],
    ) -> Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: An iterator of record dictionaries.

        Yields:
            A list of file URLs for each batch.
        """
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""
        for i, chunk in enumerate(lazy_chunked_generator(records, self.batch_config.batch_size), start=1):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                with fs.open(filename, "wb") as f, gzip.GzipFile(fileobj=f, mode="wb") as gz:
                    gz.writelines(
                        (json.dumps(record, cls=self.encoder_class, default=str) + "\n").encode()
                        for record in chunk
                    )
                file_url = fs.geturl(filename)
            yield [file_url]


class aptifyStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = aptifyConnector

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """Transform raw data to match expected structure.

        Args:
            row: A dictionary of raw record data.
            context: Optional context dictionary.

        Returns:
            A transformed record dictionary or None.
        """
        record: dict = row
        properties: dict = self.schema.get('properties')
        for key, value in record.items():
            if value is not None:
                property_schema: dict = properties.get(key)
                if isinstance(value, datetime.date):
                    record.update({key: value.isoformat()})
                if property_schema.get('contentEncoding') == 'base64':
                    record.update({key: b64encode(value).decode()})
        return record

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Optional context dictionary for partitioning.

        Yields:
            Record dictionaries.
        """
        if context:
            raise NotImplementedError(f"Stream '{self.name}' does not support partitioning.")

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)
            if replication_key_col.type.python_type in (datetime.datetime, datetime.date):
                start_val = self.get_starting_timestamp(context)
            else:
                start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        with self.connector._connect() as conn:
            for record in conn.execute(query):
                transformed_record = self.post_process(dict(record._mapping))
                if transformed_record is None:
                    continue
                yield transformed_record