"""Connector factory for creating database connectors."""

from typing import Any

from dq_platform.connectors.base import BaseConnector
from dq_platform.connectors.bigquery import BigQueryConnector
from dq_platform.connectors.databricks import DatabricksConnector
from dq_platform.connectors.duckdb_connector import DuckDBConnector
from dq_platform.connectors.mysql import MySQLConnector
from dq_platform.connectors.oracle import OracleConnector
from dq_platform.connectors.postgresql import PostgreSQLConnector
from dq_platform.connectors.redshift import RedshiftConnector
from dq_platform.connectors.snowflake import SnowflakeConnector
from dq_platform.connectors.sqlserver import SQLServerConnector
from dq_platform.models.connection import ConnectionType

CONNECTOR_MAP: dict[ConnectionType, type[BaseConnector]] = {
    ConnectionType.POSTGRESQL: PostgreSQLConnector,
    ConnectionType.MYSQL: MySQLConnector,
    ConnectionType.SQLSERVER: SQLServerConnector,
    ConnectionType.BIGQUERY: BigQueryConnector,
    ConnectionType.SNOWFLAKE: SnowflakeConnector,
    ConnectionType.REDSHIFT: RedshiftConnector,
    ConnectionType.DUCKDB: DuckDBConnector,
    ConnectionType.ORACLE: OracleConnector,
    ConnectionType.DATABRICKS: DatabricksConnector,
}


def get_connector(connection_type: ConnectionType, config: dict[str, Any]) -> BaseConnector:
    """Get a connector instance for the specified connection type.

    Args:
        connection_type: Type of database connection.
        config: Connection configuration dictionary.

    Returns:
        Connector instance.

    Raises:
        ValueError: If connection type is not supported.
    """
    connector_class = CONNECTOR_MAP.get(connection_type)
    if connector_class is None:
        raise ValueError(f"Unsupported connection type: {connection_type}")

    return connector_class(config)


def create_connector(config: dict[str, Any]) -> BaseConnector:
    """Create a connector from a configuration dictionary.

    This function extracts the connection type from the config and creates
    the appropriate connector instance.

    Args:
        config: Connection configuration dictionary containing 'connection_type'.

    Returns:
        Connector instance.

    Raises:
        ValueError: If connection type is not specified or not supported.
    """
    connection_type_str = config.get("connection_type") or config.get("type")
    if not connection_type_str:
        raise ValueError("Connection type not specified in config")

    connection_type = ConnectionType(connection_type_str)
    connector_config = {k: v for k, v in config.items() if k not in ["connection_type", "type"]}

    return get_connector(connection_type, connector_config)


class ConnectorFactory:
    """Factory class for creating database connectors."""

    @staticmethod
    def create_connector(config: dict[str, Any]) -> BaseConnector:
        """Create a connector from a configuration dictionary.

        Args:
            config: Connection configuration dictionary.

        Returns:
            Connector instance.
        """
        return create_connector(config)

    @staticmethod
    def get_connector(connection_type: ConnectionType, config: dict[str, Any]) -> BaseConnector:
        """Get a connector instance for the specified connection type.

        Args:
            connection_type: Type of database connection.
            config: Connection configuration dictionary.

        Returns:
            Connector instance.
        """
        return get_connector(connection_type, config)

    @staticmethod
    def register_connector(
        connection_type: ConnectionType,
        connector_class: type[BaseConnector],
    ) -> None:
        """Register a custom connector class.

        Args:
            connection_type: Connection type to register.
            connector_class: Connector class to use.
        """
        CONNECTOR_MAP[connection_type] = connector_class

    @staticmethod
    def list_supported_types() -> list[str]:
        """List all supported connection types.

        Returns:
            List of connection type strings.
        """
        return [ct.value for ct in CONNECTOR_MAP.keys()]
