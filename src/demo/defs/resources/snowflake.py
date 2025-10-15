from typing import Optional

import snowflake.connector
import mysql.connector
from dagster import ConfigurableResource, resource
from pydantic import Field
from ..config import config_manager  
import pandas as pd

class SnowflakeResource(ConfigurableResource):
    """Snowflake database resource for Dagster."""
    
    user: str = Field(description="Snowflake username")
    account: str = Field(description="Snowflake account identifier")
    warehouse: str = Field(description="Snowflake warehouse")
    database: str = Field(description="Snowflake database")
    schema: str = Field(description="Snowflake schema")
    role: str = Field(description="Snowflake role")
    authenticator: str = Field(default="snowflake", description="Authentication method")
    password: Optional[str] = Field(default=None, description="Snowflake password (if not using other auth)")

    def get_connection(self):
        """Create and return a Snowflake connection."""
        connection_params = {
            "user": self.user,
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "authenticator": self.authenticator,
        }
        
        if self.password:
            connection_params["password"] = self.password
            
        return snowflake.connector.connect(**connection_params)

    def execute_query(self, query: str):
        """Execute a query and return results."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_query_df(self, query: str):
        """Execute a query and return results as a pandas DataFrame."""
        return pd.read_sql(query, self.get_connection())

def create_snowflake_resource(instance_name: str = "default") -> SnowflakeResource:
    """Factory function to create Snowflake resource from configuration."""
    config = config_manager.get_snowflake_config(instance_name)
    # Filter out None values
    config = {k: v for k, v in config.items() if v is not None}
    return SnowflakeResource(**config)