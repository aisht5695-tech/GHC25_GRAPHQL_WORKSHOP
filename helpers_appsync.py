"""
Helper utilities for creating and managing an AWS AppSync GraphQL API.
Used in the GHC25 'Modeling a GraphQL Service on AWS' workshop.
"""

import boto3, time, pathlib
from datetime import datetime, timedelta, timezone


# ---------- Core client ----------
def appsync_client(region: str):
    """Return a boto3 AppSync client for the given AWS region."""
    return boto3.client("appsync", region_name=region)


# ---------- API creation ----------
def find_api_by_name(client, name: str):
    """Find an AppSync API by its name (returns dict or None)."""
    token = None
    while True:
        resp = client.list_graphql_apis(nextToken=token) if token else client.list_graphql_apis()
        for api in resp.get("graphqlApis", []):
            if api["name"] == name:
                return api
        token = resp.get("nextToken")
        if not token:
            return None


def ensure_api(client, name: str, cloudwatch_logs_role_arn: str | None = None, enable_xray: bool = False):
    """
    Create (or reuse) a GraphQL API.

    - cloudwatch_logs_role_arn: optional, provide if you want resolver logs in CloudWatch.
    - enable_xray: default False; set True only if role has iam:CreateServiceLinkedRole permissions.
    """
    api = find_api_by_name(client, name)
    if api:
        return api

    kwargs = {
        "name": name,
        "authenticationType": "API_KEY",
        "xrayEnabled": bool(enable_xray),  # avoid unauthorized SLR creation
    }

    if cloudwatch_logs_role_arn:
        kwargs["logConfig"] = {
            "cloudWatchLogsRoleArn": cloudwatch_logs_role_arn,
            "fieldLogLevel": "ERROR",
        }

    return client.create_graphql_api(**kwargs)["graphqlApi"]


# ---------- Schema ----------
def upload_schema(client, api_id: str, schema_path: str):
    """Upload a .graphql schema to AppSync and wait for compilation."""
    data = pathlib.Path(schema_path).read_bytes()
    client.start_schema_creation(apiId=api_id, definition=data)
    for _ in range(30):
        st = client.get_schema_creation_status(apiId=api_id)
        status, details = st.get("status"), st.get("details", "")
        if status in ("SUCCESS", "FAILED", "NOT_APPLICABLE"):
            if status != "SUCCESS":
                raise RuntimeError(f"Schema status={status}: {details[:400]}")
            return
        time.sleep(2)


# ---------- API key ----------
def ensure_api_key(client, api_id: str, days=1):
    """
    Create a temporary API key.
    AppSync requires >=1 and <=365 days validity.
    """
    if days < 1:
        days = 1
    expires_epoch = int((datetime.now(timezone.utc) + timedelta(days=days, minutes=2)).timestamp())
    key = client.create_api_key(apiId=api_id, description="workshop", expires=expires_epoch)["apiKey"]
    return key["id"], key["expires"]


# ---------- Data sources ----------
def ensure_ddb_ds(client, api_id: str, name: str, table_arn: str, role_arn: str):
    """Create or reuse a DynamoDB data source."""
    ds = client.list_data_sources(apiId=api_id).get("dataSources", [])
    for d in ds:
        if d["name"] == name:
            return d
    return client.create_data_source(
        apiId=api_id,
        name=name,
        type="AMAZON_DYNAMODB",
        serviceRoleArn=role_arn,
        dynamodbConfig={
            "tableName": table_arn.split("/")[-1],
            "awsRegion": table_arn.split(":")[3],
            "useCallerCredentials": False,
        },
    )["dataSource"]


def ensure_none_ds(client, api_id: str, name="NoneDS"):
    """Create or reuse a NONE data source (used for synthetic resolvers)."""
    ds = client.list_data_sources(apiId=api_id).get("dataSources", [])
    for d in ds:
        if d["name"] == name:
            return d
    return client.create_data_source(apiId=api_id, name=name, type="NONE")["dataSource"]


# ---------- JS resolvers ----------
def upsert_js_resolver(client, api_id: str, type_name: str, field: str, ds_name: str, js_code: str):
    """Create or update a JavaScript resolver for the specified field."""
    try:
        client.update_resolver(
            apiId=api_id,
            typeName=type_name,
            fieldName=field,
            dataSourceName=ds_name,
            runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
            code=js_code,
        )
    except client.exceptions.NotFoundException:
        client.create_resolver(
            apiId=api_id,
            typeName=type_name,
            fieldName=field,
            dataSourceName=ds_name,
            runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
            code=js_code,
        )

# ---------- Get DDB Query Resolver JS for Cost ---------
def get_ddb_query_resolver_js_code():
    """Return JavaScript code for DynamoDB cost query resolver."""
    return """
export function request(ctx) {
  const asin = ctx.source?.asin;
  const vendorId = ctx.args?.vendorId;
  
  if (!asin) {
    util.error("Missing ASIN from parent", "BadRequest");
  }
  if (!vendorId) {
    util.error("Missing vendorId argument", "BadRequest");
  }
  
  const asinVendor = `${asin}#${vendorId}`;
  
  return {
    operation: "GetItem",
    key: util.dynamodb.toMapValues({ asinVendor: asinVendor })
  };
}

export function response(ctx) {
  if (ctx.error) {
    util.error(ctx.error.message, ctx.error.type);
  }
  
  return ctx.result || null;
}
"""

# ---------- Get DDB Mutation Resolver JS for Cost ---------
def get_ddb_mutation_resolver_js_code():
    """Return JavaScript code for DynamoDB cost mutation resolver."""
    return """
export function request(ctx) {
  const { asin, costData } = ctx.args;
  if (!asin) util.error("asin is required", "BadRequest");
  if (!costData?.vendorId) util.error("costData.vendorId is required", "BadRequest");

  const updatedAt = costData.updatedAt ?? util.time.nowISO8601();
  const asinVendor = `${asin}#${costData.vendorId}`;
  const item = {
    asinVendor,
    vendorId: costData.vendorId,
    cost: costData.cost,
    currency: costData.currency,
    updatedAt,
  };

  return {
    operation: "PutItem",
    key: util.dynamodb.toMapValues({ asinVendor }),
    attributeValues: util.dynamodb.toMapValues(item),
  };
}

export function response(ctx) {
  if (ctx.error) util.error(ctx.error.message, ctx.error.type);

  // Store result for next function
  ctx.stash.writtenData = { ...ctx.args.costData };
  ctx.stash.writtenData.updatedAt = ctx.stash.writtenData.updatedAt || util.time.nowISO8601();
  return ctx.stash.writtenData;
}
"""

# ---------- Get SNS Mutation Resolver JS for Cost ---------
def get_sns_mutation_resolver_js_code(topic_arn):
    """Return JavaScript code for SNS cost mutation resolver."""
    return f"""
export function request(ctx) {{
  const asin = ctx.args.asin;
  const costData = ctx.stash.writtenData || ctx.args.costData;

  if (!asin || !costData) {{
    util.error("Missing asin or costData in SNS function", "BadRequest");
  }}

  const arn = util.urlEncode("{topic_arn}");
  const message = util.urlEncode(JSON.stringify({{
    event: "cost_updated",
    asin: asin,
    vendorId: costData.vendorId,
    cost: costData.cost,
    currency: costData.currency,
    timestamp: util.time.nowISO8601()
  }}));
  
  const parts = [
    'Action=Publish',
    'Version=2010-03-31',
    `TopicArn=${{arn}}`,
    `Message=${{message}}`
  ]
  
  const body = parts.join('&');
  return {{
    version: "2018-05-29",
    method: "POST",
    resourcePath: "/",
    params: {{
      body,
      headers: {{
        "content-type": "application/x-www-form-urlencoded"
      }}
    }}
  }};
}}

export function response(ctx) {{
  console.log("SNS Response:", JSON.stringify(ctx.result));
  if (ctx.error) {{
    console.log("SNS Error:", JSON.stringify(ctx.error));
    util.error(ctx.error.message, ctx.error.type);
  }}
  return ctx.prev.result;
}}
"""
