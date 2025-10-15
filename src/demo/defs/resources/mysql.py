from typing import Optional

import snowflake.connector
import mysql.connector
from dagster import ConfigurableResource, resource
from pydantic import Field
from ..config import config_manager  
import pandas as pd

class MySQLResource(ConfigurableResource):
    """MySQL database resource for Dagster."""
    
    host: str = Field(description="MySQL host")
    port: int = Field(default=3306, description="MySQL port")
    user: Optional[str] = Field(default=None, description="MySQL username")
    password: Optional[str] = Field(default=None, description="MySQL password")
    database: str = Field(description="MySQL database name")

    def get_connection(self):
        """Create and return a MySQL connection."""
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            connection_timeout=6000,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci',
            init_command=f'SET SESSION max_statement_time=6000'
        )

    def execute_query(self, query: str):
        """Execute a query and return results."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
            
    def execute_query_df(self, query: str):
        """Execute a query and return results as a pandas DataFrame."""
        return pd.read_sql(query, self.get_connection())


def create_mysql_resource(instance_name: str = "default") -> MySQLResource:
    """Factory function to create MySQL resource from configuration."""
    config = config_manager.get_mysql_config(instance_name)
    # Filter out None values
    config = {k: v for k, v in config.items() if v is not None}
    return MySQLResource(**config)