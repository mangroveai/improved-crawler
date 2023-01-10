from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_glue as glue
from aws_cdk import aws_glue_alpha as glue_alpha
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import custom_resources as cr
from constructs import Construct

RAW_TABLE = "db"
S3_MISC_PATH = "misc/"


class CrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        crawler_event_queue = sqs.CfnQueue(self, "MyCfnQueue")

        crawler_event_topic = sns.CfnTopic(
            self,
            "MyCfnTopic",
            subscription=[
                sns.CfnTopic.SubscriptionProperty(
                    endpoint=str(crawler_event_queue.get_att("arn")), protocol="sqs"
                )
            ],
        )

        bucket = s3.CfnBucket(
            self,
            "bucket",
            notification_configuration=s3.CfnBucket.NotificationConfigurationProperty(
                event_bridge_configuration=s3.CfnBucket.EventBridgeConfigurationProperty(
                    event_bridge_enabled=False
                ),
                # topic_configurations=[
                #    s3.CfnBucket.TopicConfigurationProperty(
                #        event="event",
                #        topic=crawler_event_topic.ref,
                #    )
                # ],
            ),
        )

        glue_database = glue_alpha.Database(self, "GlueDB", database_name="test_db")

        assume_role_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["s3:GetBucket*", "s3:GetObject*", "s3:List*"],
                    "Resource": [
                        "*",
                    ],
                    "Effect": "Allow",
                },
                {
                    "Action": ["cloudwatch:PutMetricData", "glue:*"],
                    "Resource": "*",
                    "Effect": "Allow",
                },
                {
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    "Resource": "arn:aws:logs:*:*:/aws-glue/*",
                    "Effect": "Allow",
                },
                {
                    "Action": [
                        "*",
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                },
            ],
        }

        crawler_role = iam.CfnRole(
            self,
            "crawler_role",
            assume_role_policy_document=assume_role_policy_document,
            policies=[
                iam.CfnRole.PolicyProperty(
                    policy_document=policy_document, policy_name="crawler_policy"
                )
            ],
        )
        print(
            "AAAAAAAAAAAA----------------------------------------------------------------------------------------------------------------------------"
        )
        print(str(crawler_event_queue.get_att("arn")))
        crawler = glue.CfnCrawler(
            self,
            "crawler",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/{S3_MISC_PATH}{RAW_TABLE}/",
                        event_queue_arn=str(crawler_event_queue.get_att("arn")),
                    )
                ]
            ),
            role=str(crawler_role.get_att("arn")).split("/")[0],
            database_name=glue_database.database_name,
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVENT_MODE"
            ),
        )

        aws_custom = cr.AwsCustomResource(
            self,
            "bucket notificaation",
            on_create=cr.AwsSdkCall(
                service="S3",
                action="putBucketNotificationConfiguration",
                parameters={
                    "Bucket": bucket.bucket_name,
                    "NotificationConfiguration": {
                        "TopicConfigurations": [
                            {
                                "Events": ["s3:ObjectCreated:*"],
                                "Topicarn": str(crawler_event_topic.get_att("arn")),
                                "Filter": {
                                    "Key": {
                                        "FilterRules": [
                                            {
                                                "Name": "prefix",
                                                "Value": f"{S3_MISC_PATH}{RAW_TABLE}/",
                                            },
                                        ]
                                    }
                                },
                                "Id": crawler.ref,
                            }
                        ]
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of("notif-" + crawler.ref),
            ),
            on_delete=cr.AwsSdkCall(
                service="S3",
                action="putBucketNotificationConfiguration",
                parameters={
                    "Bucket": bucket.bucket_name,
                    "NotificationConfiguration": {},
                },
                physical_resource_id=cr.PhysicalResourceId.of("notif-" + crawler.ref),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                statements=[
                    iam.PolicyStatement(
                        actions=["s3:PutBucketNotification*"], resources=["*"]
                    )
                ]
            ),
        )
