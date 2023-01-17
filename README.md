# improved-crawler
A CDK example of a crawler that works like in : https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html

To deploy the project you will just have to do :
```
cdk deploy
```

To test the solution, you can upload a json (for example example_json) in bucket/db/

You can then run teh crawler or the Glue Workflow to test them
