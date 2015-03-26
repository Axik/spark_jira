import time
import datetime
from dateutil.parser import parse as dateparse

from jira import JIRA
import pandas as pd

headline_names = ['Opener', 'Assignee', 'Type', 'Summary',
                  'Opened', 'Closed']


def get_closed_issues():
    options = {
        'server': 'https://issues.apache.org/jira'
    }

    jira = JIRA(options)
    jql_project = 'project=SPARK AND status in (Resolved, Closed)'
    result_issues = []
    page = 0
    while True:
        result = jira.search_issues(jql_str=jql_project,
                                    startAt=page, maxResults=100)
        if len(result) < 100:
            result_issues.extend(result)
            break
        result_issues.extend(result)
        page += 100
        # time.sleep(2.5)
    return result_issues


def get_property(obj, property_path):
    if len(property_path) == 1 and hasattr(obj, property_path[0]):
        return getattr(obj, property_path[0])
    if hasattr(obj, property_path[0]):
        return get_property(getattr(obj, property_path[0]), property_path[1:])
    else:
        return None


def collect_dict_from_issue(issue):
    keys = headline_names[:]
    vals = [get_property(issue, ['fields', 'creator', 'name']),
            get_property(issue, ['fields', 'assignee', 'name']),
            get_property(issue, ['fields', 'issuetype', 'name']),
            get_property(issue, ['fields', 'summary']),
            get_property(issue, ['fields', 'created']),
            get_property(issue, ['fields', 'updated'])]
    return get_with_valid_date(dict(zip(keys, vals)))

valid_start_date = datetime.datetime(year=2010, month=1, day=1, tzinfo=pytz.UTC)
valid_end_date = datetime.datetime(year=2015, month=4, day=1, tzinfo=pytz.UTC)


def get_with_valid_date(d):
    if d.get('Opened') is not None and d.get('Closed'):
        opened = dateparse(d.get('Opened'))
        closed = dateparse(d.get('Closed'))
        if valid_start_date <= opened <= valid_end_date and \
                valid_start_date <= closed <= valid_end_date:
            d['Opened'] = opened.strftime('%Y/%m/%d %H:%M:%S')
            d['Closed'] = closed.strftime('%Y/%m/%d %H:%M:%S')
            d['Spendtime'] = (closed - opened).seconds
            return d


def prepare_list_for_csv(issues):
    return list(filter(lambda x: x is not None, map(collect_dict_from_issue, issues)))


def write_csv_from_list_of_dicts(lst):
    df = pd.DataFrame.from_records(lst)
    filename = 'spark_jira_closed_issues_{}.csv'.format(time.time())
    df.to_csv(filename, encoding='utf-8', index=False)
    # with open(filename, 'w') as csvfile:
    #     fieldnames = headline_names[:]
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for row in lst:
    #         writer.writerow(row)


if __name__ == '__main__':
    issues = get_closed_issues()
    csv_list = prepare_list_for_csv(issues)
    write_csv_from_list_of_dicts(csv_list)
