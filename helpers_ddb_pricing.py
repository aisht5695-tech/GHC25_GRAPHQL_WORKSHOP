import boto3, json
from datetime import datetime
def iso(): return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# Catalog: PK = asin
def ensure_catalog_table(table_name: str, region: str):
    ddb = boto3.client("dynamodb", region_name=region)
    try:
        return ddb.describe_table(TableName=table_name)["Table"]
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName":"asin","AttributeType":"S"}],
        KeySchema=[{"AttributeName":"asin","KeyType":"HASH"}],
        BillingMode="PAY_PER_REQUEST",
        SSESpecification={"Enabled": True},
    )
    ddb.get_waiter("table_exists").wait(TableName=table_name)
    return ddb.describe_table(TableName=table_name)["Table"]

# Cost: PK = asinVendor (join key "ASIN#VendorId")
def ensure_cost_table(table_name: str, region: str):
    ddb = boto3.client("dynamodb", region_name=region)
    try:
        return ddb.describe_table(TableName=table_name)["Table"]
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName":"asinVendor","AttributeType":"S"}],
        KeySchema=[{"AttributeName":"asinVendor","KeyType":"HASH"}],
        BillingMode="PAY_PER_REQUEST",
        SSESpecification={"Enabled": True},
    )
    ddb.get_waiter("table_exists").wait(TableName=table_name)
    return ddb.describe_table(TableName=table_name)["Table"]

def seed_catalog(table_name: str, region: str, asin: str):
    ddb = boto3.client("dynamodb", region_name=region)
    item = {
        "asin": {"S": asin},
        "title": {"S": "Laptop 13”"},
        "brand": {"S": "Acme"},
        "category": {"S": "Computers"},
        "description": {"S": "Thin-and-light 13-inch laptop"},
        "defaultCurrency": {"S": "USD"},
        "updatedAt": {"S": iso()},
    }
    ddb.put_item(TableName=table_name, Item=item)

def seed_cost(table_name: str, region: str, asin: str):
    ddb = boto3.client("dynamodb", region_name=region)
    rows = [
        ("AcmeSupply", 865.00, "USD"),
        ("GlobalParts", 848.50, "USD"),
    ]
    for vendor, cost, ccy in rows:
        pk = f"{asin}#{vendor}"
        item = {
            "asinVendor": {"S": pk},
            "vendorId": {"S": vendor},
            "cost": {"N": f"{cost:.2f}"},
            "currency": {"S": ccy},
            "updatedAt": {"S": iso()},
        }
        ddb.put_item(TableName=table_name, Item=item)
