from config import get_config, ApplicationConfig, ConnectionConfig, SyncConfig


def test_config_parsing():
    test_config = get_config("application.conf")
    assert test_config == ApplicationConfig(
        connection=ConnectionConfig(
            host="localhost",
            port="27017",
            username="docdbuser",
            password="Admin_123",
            options={}
        ),
        syncs=[
            SyncConfig(
                source_database="business",
                source_collections=["restaurants"],
                destination_schema="docdb_raw"
            )
        ]
    )


def test_connection_string():
    test_config = get_config("application.conf")
    assert test_config.connection.build_connection_string() == \
           "mongodb://docdbuser:Admin_123@localhost:27017"
