from dagster import asset
import pandas as pd

@asset(group_name="demo", 
       description="Users information")
def raw_demo_user() -> pd.DataFrame:
    return pd.DataFrame({"name": ["test1", "test2", "test3"], "age": [1, 2, 3], "height": [1.81, 1.75, 1.80], "weight": [70, 65, 75]})

@asset(group_name="demo", 
       description="Users address")
def raw_demo_user_address() -> pd.DataFrame:
    return pd.DataFrame({"name": ["test1", "test2", "test3"], "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"]})

@asset(group_name="demo", 
       description="Silver user information")
def silver_demo_user(raw_demo_user: pd.DataFrame) -> pd.DataFrame:
    raw_demo_user['bmi'] = raw_demo_user['weight'] / (raw_demo_user['height'] ** 2)
    return raw_demo_user

@asset(group_name="demo", 
       description="Gold user information")
def gold_user_info(raw_demo_user_address: pd.DataFrame, silver_demo_user: pd.DataFrame) -> pd.DataFrame:
    user_info = pd.merge(raw_demo_user_address, silver_demo_user, on="name")
    return user_info