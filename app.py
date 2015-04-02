from flask import Flask
from flask import render_template
from flask_bootstrap import Bootstrap

from init import prepare_list_for_csv
from jira import JIRA

import pandas as pd
from datarobot_sdk import Project, Model

jira_board = JIRA({
    'server': 'https://issues.apache.org/jira'
})


def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    return app

app = create_app()

dr_project = Project.list()[0]


def get_opened_jira_tasks(page=0):
    issues = jira_board.search_issues(
        jql_str='project=SPARK AND status in (Open)',
        startAt=page * 50, maxResults=50)
    return prepare_list_for_csv(issues)


@app.route('/', defaults={'page': 0})
@app.route('/<int:page>')
def hello(page):
    model = Model.get(dr_project, '551428c5133127267774d900')
    jira_tasks = get_opened_jira_tasks(page)
    df = pd.DataFrame.from_records(jira_tasks)
    preds = model.predict(df).prediction.tolist()

    context = {'opened_tasks': zip(jira_tasks, preds),
               'next_page': page + 1,
               'prev_page': page - 1 if page != 0 else None}
    return render_template('main.html', **context)


if __name__ == "__main__":
    app.run(debug=True)
