from dataclasses import dataclass

from pyhocon import ConfigFactory


@dataclass
class ConnectionConfig:
    host: str
    port: str
    username: str
    password: str
    options: dict[str, str]

    def build_connection_string(self):
        connection_str = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}"

        if self.options:
            connection_str += "/?"
            for key, value in self.options.items():
                connection_str += f"{key}={value}&"

            connection_str = connection_str[:-1]

        return connection_str


@dataclass
class SyncConfig:
    source_database: str
    source_collections: list[str]
    destination_schema: str


@dataclass
class ApplicationConfig:
    connection: ConnectionConfig
    syncs: list[SyncConfig]


def get_config(application_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_path)

    connection = ConnectionConfig(
        host=config.get_string("connection.host"),
        port=config.get_string("connection.port"),
        username=config.get_string("connection.username"),
        password=config.get_string("connection.password"),
        options=config.get_config("connection.options", {})
    )

    syncs = []
    for sync in config["syncs"] or []:
        syncs.append(
            SyncConfig(
                source_database=sync.get_string("source_database"),
                source_collections=sync.get_list("source_collections"),
                destination_schema=sync.get_string("destination_schema")
            )
        )

    return ApplicationConfig(
        connection=connection,
        syncs=syncs
    )
