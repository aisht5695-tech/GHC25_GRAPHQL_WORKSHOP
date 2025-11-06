import boto3
import json
from datetime import datetime

def iso(): 
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def ensure_sns_topic(api_name: str, region: str):
    """Create SNS topic using API naming convention, return topic ARN"""
    topic_name = f"{api_name}-price-changes"
    sns = boto3.client('sns', region_name=region)
    try:
        response = sns.create_topic(Name=topic_name)
        return response['TopicArn']
    except Exception as e:
        if "already exists" in str(e).lower():
            topics = sns.list_topics()['Topics']
            return next((t['TopicArn'] for t in topics if topic_name in t['TopicArn']), None)
        raise e

def ensure_sqs_queue(api_name: str, region: str):
    """Create SQS queue using API naming convention, return queue URL and ARN"""
    queue_name = f"{api_name}-price-processing"
    sqs = boto3.client('sqs', region_name=region)
    try:
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'MessageRetentionPeriod': '1209600',  # 14 days
                'VisibilityTimeout': '60'
            }
        )
        queue_url = response['QueueUrl']
        
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = attrs['Attributes']['QueueArn']
        
        return queue_url, queue_arn
        
    except Exception as e:
        if "already exists" in str(e).lower():
            queues = sqs.list_queues(QueueNamePrefix=queue_name)
            if 'QueueUrls' in queues and queues['QueueUrls']:
                queue_url = queues['QueueUrls'][0]
                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['QueueArn']
                )
                queue_arn = attrs['Attributes']['QueueArn']
                return queue_url, queue_arn
        raise e

def setup_sns_sqs_subscription(topic_arn: str, queue_arn: str, queue_url: str, region: str):
    """Set up SQS policy and SNS subscription"""
    sns = boto3.client('sns', region_name=region)
    sqs = boto3.client('sqs', region_name=region)
    
    # Set SQS policy to allow SNS
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}}
        }]
    }
    
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'Policy': json.dumps(policy)}
    )
    
    # Subscribe SQS to SNS
    subscription = sns.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )
    
    return subscription['SubscriptionArn']

def publish_price_event(topic_arn: str, api_basename: str, asin: str, vendor_id: str, 
                       old_cost: float, new_cost: float, currency: str = "USD", 
                       reason: str = "Price update", region: str = "us-east-1"):
    """Publish a price change event to SNS"""
    sns = boto3.client('sns', region_name=region)
    
    event = {
        'eventType': 'PRICE_UPDATED',
        'attendee': api_basename,
        'asin': asin,
        'vendorId': vendor_id,
        'oldCost': old_cost,
        'newCost': new_cost,
        'currency': currency,
        'timestamp': iso(),
        'changeReason': reason
    }
    
    response = sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(event, indent=2),
        Subject=f'Price Update - {api_basename}'
    )
    
    return response['MessageId'], event
