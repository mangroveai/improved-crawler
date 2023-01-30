# improved-crawler
A CDK example of a crawler that works like in : https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html

The python version of this project is 3.10 or further
To deploy the project you will just have to do :
```
curl -sSL https://install.python-poetry.org | python3 -
```
To install poetry.
```
poetry install
```
to install the required libraries and then :
```
poetry shell
```
To actvate the virtual environment.
```
cdk deploy
```

To test the solution, you can upload a json (for example example_json) in bucket/db/

Then run the Glue Workflow a first time. For the following commands, the name of the workflow and of the crawler are displayed as output of the stak
```
start-workflow-run --name <your workflow name>
```
To see the results of this first launch
```
aws logs tail /aws-glue/crawlers --log-stream-names <you crawler name> --follow
```
If all went right, you should see that the crawler did something (the number of unique events received should be 1 or more)

You can then redo these two commands to see what the second execution is doing
```
start-workflow-run --name <your workflow name>
```
```
aws logs tail /aws-glue/crawlers --log-stream-names <you crawler name> --follow
```
Now the number of unique events should be 0 and the crawler should have done nothing
To then delete the stack :
```
cdk destroy
```