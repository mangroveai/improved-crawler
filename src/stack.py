from aws_cdk import Duration, Stack
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
REPARTITIONED_TABLE = "db_repartitioned"


class CrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, "bucket", event_bridge_enabled=True)

        glue_database = glue_alpha.Database(self, "GlueDB", database_name="db")

        crawler_role = iam.Role(
            self,
            "crawler_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        bucket.grant_read(crawler_role)
        crawler_role.add_to_policy(
            iam.PolicyStatement(actions=["glue:*"], resources=["*"])
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(actions=["cloudwatch:PutMetricData"], resources=["*"])
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:/aws-glue/*"],
            )
        )
        crawler_event_queue = sqs.Queue(self, "Crawler Queue")

        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["*"],
                resources=[crawler_event_queue.queue_arn],
            )
        )

        crawler_event_queue.grant_consume_messages(crawler_role)
        crawler_event_topic = sns.Topic(self, "Topic crawler")
        crawler_event_topic.add_subscription(
            subscriptions.SqsSubscription(crawler_event_queue)
        )
        bucket.add_object_created_notification(
            s3n.SnsDestination(crawler_event_topic),
            s3.NotificationKeyFilter(prefix="{S3_MISC_PATH}{RAW_TABLE}/"),
        )

        crawler = glue.CfnCrawler(
            self,
            "crawler",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/{S3_MISC_PATH}{RAW_TABLE}/",
                        event_queue_arn=crawler_event_queue.queue_arn,
                    )
                ]
            ),
            role=crawler_role.role_arn,
            database_name=glue_database.database_name,
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVENT_MODE"
            ),
        )
        crawler.node.add_dependency(crawler_role)

        job = glue_alpha.Job(
            self,
            "Job",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V3_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_asset("glue_job/parquet.py"),
            ),
            timeout=Duration.minutes(15),
            max_capacity=2,
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--bucket": bucket.bucket_name,
                "--db": glue_database.database_name,
                "--raw_table": RAW_TABLE,
                "--repartitioned_table": REPARTITIONED_TABLE,
                "--s3_path": f"/{S3_MISC_PATH}{REPARTITIONED_TABLE}/",
            },
        )
        bucket.grant_read_write(job)

        workflow = glue.CfnWorkflow(self, "Workflow")

        trigger_crawler = glue.CfnTrigger(
            self,
            "Trigger Crawler",
            actions=[glue.CfnTrigger.ActionProperty(crawler_name=crawler.ref)],
            type="SCHEDULED",
            workflow_name=workflow.ref,
            schedule="cron(0 */6 * * ? *)",
            start_on_creation=True,
        )

        trigger_job = glue.CfnTrigger(
            self,
            "Trigger Job",
            actions=[glue.CfnTrigger.ActionProperty(job_name=job.job_name)],
            type="CONDITIONAL",
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=crawler.ref,
                        crawl_state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ],
            ),
            start_on_creation=True,
            workflow_name=workflow.ref,
        )

        topic = sns.Topic(self, "Topic Image Ready")

        dead_letter_queue = sqs.Queue(self, "Dead Letter Queue")
        dlq = sqs.DeadLetterQueue(max_receive_count=3, queue=dead_letter_queue)
        queue = sqs.Queue(self, "Queue", dead_letter_queue=dlq)
        topic.add_subscription(subscriptions.SqsSubscription(queue))

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
                                "TopicArn": crawler_event_topic.topic_arn,
                                "Filter": {
                                    "Key": {
                                        "FilterRules": [
                                            {
                                                "Name": "prefix",
                                                "Value": "misc/db/",
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
