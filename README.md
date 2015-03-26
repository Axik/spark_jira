## DataRobot SDK Software Engineer usecase.


 - Build web-app that will display prediction dashboard with list of opened issue with predicted date when it will be closed.

    | Issue   | Expected fix data |
    | ------- | ----------------- |
    | DR-9999 | 08.04.2015        |


According to date input(see below) we can build a model, that will predict when probably this issue will be closed. Input data will be pulled through JIRA API.

Data input:

 - Who opened issue issue.fields.creator.name
 - Type issue issue.fields.issuetype.name
 - Description iss.fields.description
 - Who was assigned issue.fields.assignee.name
 - When opened iss.fields.created
 - When closed iss.fields.updated

Prediction:

 - If it's not closed predict when it will closed.

How it should looks for engineers:

 - Easy flask app example (one file) that using JIRA api to pull data, and datarobot_sdk python package to build model and get a prediction.
