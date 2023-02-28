# improved-crawler
A CDK example of a Glue Crawler based on [S3 event notifications](https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html).

Here is a look at what is being deployed by this stack :
<img width="622" alt="image" src="https://user-images.githubusercontent.com/48856634/221613582-fdb4bcdd-7dcf-4295-a434-a777e3a95a5b.png">

## Deployment
Install poetry:
```
curl -sSL https://install.python-poetry.org | python3 -
```

Create Python environment:
```
# The python version of this project is 3.10 or further.
poetry install
```
Activate the virtual environment:
```
poetry shell
```

Deploy the stack containing the crawler, a bucket and a Glue workflow to run the crawler:
```
# It is assumed that you have valid AWS credentials
cdk deploy
```

## Test it!
To test the solution, you can upload a json (for example example_json) in the bucket:
```
aws s3 cp example_json s3://<your bucket name>
```

Then run the Glue Workflow a first time. For the following commands, the name of the workflow and of the crawler are displayed as output of the stak
```
aws glue start-workflow-run --name <your workflow name>
```
To see the results of this first launch
```
aws logs tail /aws-glue/crawlers --log-stream-names <you crawler name> --follow
```
If all went right, you should have something like that:
![cli logs drawio](https://user-images.githubusercontent.com/48856634/221797944-eea8c7ca-b0bf-4668-b26c-0beb8a0a9177.png)

You can then redo these two commands to see what the second execution is doing
```
aws glue start-workflow-run --name <your workflow name>
```
```
aws logs tail /aws-glue/crawlers --log-stream-names <you crawler name> --follow
```
Now the number of unique events should be 0 and the crawler should have done nothing:
![cli logs2 drawio](https://user-images.githubusercontent.com/48856634/221798333-7c72d7c0-ad86-4258-8c7f-726cf7bc4370.png)

## Clean up
To then delete the stack :
```
cdk destroy
```
