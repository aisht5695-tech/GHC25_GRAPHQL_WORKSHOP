def create_pipeline_resolver(client, api_id: str, type_name: str, field: str, functions: list):
    """Create a pipeline resolver with multiple functions."""
    try:
        client.update_resolver(
            apiId=api_id,
            typeName=type_name,
            fieldName=field,
            kind="PIPELINE",
            requestMappingTemplate="{}",
            responseMappingTemplate="$util.toJson($ctx.result)",
            pipelineConfig={"functions": functions}
        )
    except client.exceptions.NotFoundException:
        client.create_resolver(
            apiId=api_id,
            typeName=type_name,
            fieldName=field,
            kind="PIPELINE",
            requestMappingTemplate="{}",
            responseMappingTemplate="$util.toJson($ctx.result)",
            pipelineConfig={"functions": functions}
        )
        
def create_function(client, api_id: str, name: str, ds_name: str, js_code: str):
    """Create or update an AppSync function."""
    try:
        return client.update_function(
            apiId=api_id,
            functionId=name,
            name=name,
            dataSourceName=ds_name,
            runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
            code=js_code
        )["functionConfiguration"]
    except client.exceptions.NotFoundException:
        return client.create_function(
            apiId=api_id,
            name=name,
            dataSourceName=ds_name,
            runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
            code=js_code
        )["functionConfiguration"]

def ensure_sns_ds(client, api_id: str, name: str, topic_arn: str, role_arn: str):
    """Create or reuse an SNS data source via HTTP."""
    ds = client.list_data_sources(apiId=api_id).get("dataSources", [])
    for d in ds:
        if d["name"] == name:
            return d

    region = topic_arn.split(':')[3]
    return client.create_data_source(
        apiId=api_id,
        name=name,
        type="HTTP",
        serviceRoleArn=role_arn,
        httpConfig={
            "endpoint": f"https://sns.{region}.amazonaws.com/",
            "authorizationConfig": {
                "authorizationType": "AWS_IAM",
                "awsIamConfig": {
                    "signingRegion": region,
                    "signingServiceName": "sns"
                }
            }
        }
    )["dataSource"]