from aws_cdk import Duration, Stack, CfnOutput
from aws_cdk import aws_glue as glue
from aws_cdk import aws_glue_alpha as glue_alpha
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import custom_resources as cr
from constructs import Construct

RAW_TABLE = "db"
NEW_TABLE = "new_table"
# The S3 path where you will upload your data will be "s3://{bucket.bucket_name}/


class CrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, "bucket")

        glue_database = glue_alpha.Database(self, "GlueDB", database_name=RAW_TABLE)

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

        topic_policy = sns.TopicPolicy(
            self, "TopicPolicy", topics=[crawler_event_topic]
        )

        topic_policy.document.add_statements(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                resources=[crawler_event_topic.topic_arn],
                conditions={"ArnLike": {"aws:SourceArn": bucket.bucket_arn}},
            )
        )

        crawler = glue.CfnCrawler(
            self,
            "crawler",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/",
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
        crawler.node.add_dependency(
            crawler_role
        )  # Important as the dependency is not integrated natively in CDK/Cloudformation

        job = glue_alpha.Job(
            self,
            "Job",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V3_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_asset("glue_job/job.py"),
            ),
            timeout=Duration.minutes(15),
            max_capacity=2,
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--bucket": bucket.bucket_name,
                "--db": glue_database.database_name,
                "--raw_table": RAW_TABLE,
                "--repartitioned_table": NEW_TABLE,
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
            schedule="cron(0 */6 * * ? *)",  # every 6 hours
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

        aws_custom = cr.AwsCustomResource(
            self,
            "bucket notification",
            on_create=cr.AwsSdkCall(
                service="S3",
                action="putBucketNotificationConfiguration",
                parameters={
                    "Bucket": bucket.bucket_name,
                    "NotificationConfiguration": {
                        "TopicConfigurations": [
                            {
                                "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                                "TopicArn": crawler_event_topic.topic_arn,
                                "Id": crawler.ref,  # important part if you want the crawler to read the event
                            }
                        ],
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of("notif-" + crawler.ref),
            ),
            on_update=cr.AwsSdkCall(
                service="S3",
                action="putBucketNotificationConfiguration",
                parameters={
                    "Bucket": bucket.bucket_name,
                    "NotificationConfiguration": {
                        "TopicConfigurations": [
                            {
                                "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                                "TopicArn": crawler_event_topic.topic_arn,
                                "Id": crawler.ref,  # important part if you want the crawler to read the event
                            }
                        ],
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of("notif-" + crawler.ref),
            ),
            on_delete=cr.AwsSdkCall(
                service="S3",
                action="putBucketNotificationConfiguration",
                parameters={
                    "Bucket": bucket.bucket_name,
                    "NotificationConfiguration": {},  # Be careful, it will delete all of your bucket notifs
                },
                physical_resource_id=cr.PhysicalResourceId.of("notif-" + crawler.ref),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                statements=[
                    iam.PolicyStatement(
                        actions=["s3:PutBucketNotification*"], resources=["*"]
                    ),
                ]
            ),
        )
        CfnOutput(self, "crawlerName", value=crawler.ref)
        CfnOutput(self, "workflowName", value=workflow.ref)
        CfnOutput(self, "bucketName", value=bucket.bucket_name)
