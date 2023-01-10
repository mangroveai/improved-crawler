import aws_cdk as cdk

from src.stack import CrawlerStack

app = cdk.App()
stack = CrawlerStack(app, "CrawlerStack")
cdk.Tags.of(stack).add("Application", "Automatic Crawler")

app.synth()
