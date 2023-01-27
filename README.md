# improved-crawler
A CDK example of a crawler that works like in : https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html

The python version of this project is 3.10 or further
To deploy the project you will just have to do :
```
curl -sSL https://install.python-poetry.org | python3 -
```
To install poetry.
```
poetry intsall
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

You can then run teh crawler or the Glue Workflow to test them
