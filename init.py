import time
import datetime
from dateutil.parser import parse as dateparse

from jira import JIRA
import pandas as pd
import argparse

from datarobot_sdk import Project

headline_names = ['Opener', 'Assignee', 'Type', 'Priority', 'Summary',
                  'Opened', 'Closed']


def get_closed_issues(jira_link='https://issues.apache.org/jira',
                      project_name='SPARK', limit=None, auth=None):
    options = {
        'server': jira_link
    }
    if not all(auth):
        auth = None
    jira = JIRA(options, basic_auth=auth)
    jql_project = 'project = {} AND status in (Resolved, Closed)'.format(
        project_name)
    result_issues = []
    page = 0
    while True:
        result = jira.search_issues(jql_str=jql_project,
                                    startAt=page, maxResults=100)
        result_issues.extend(result)

        if len(result) < 100:
            break
        if limit and len(result_issues) > limit:
            break
        page += 100
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
            get_property(issue, ['fields', 'priority', 'name']),
            get_property(issue, ['fields', 'summary']),
            get_property(issue, ['fields', 'created']),
            get_property(issue, ['fields', 'updated'])]
    return get_with_valid_date(dict(zip(keys, vals)))


def get_with_valid_date(d):
    if d.get('Opened') is not None and d.get('Closed'):
        opened = dateparse(d.get('Opened'))
        closed = dateparse(d.get('Closed'))
        valid_start_date = datetime.datetime(year=2010, month=1, day=1,
                                             tzinfo=opened.tzinfo)
        valid_end_date = datetime.datetime.now().replace(tzinfo=opened.tzinfo)
        if valid_start_date <= opened <= valid_end_date and \
                valid_start_date <= closed <= valid_end_date:
            d['Opened'] = opened.strftime('%Y/%m/%d %H:%M:%S')
            d['Spendtime'] = (closed - opened).total_seconds() / 60 / 60 / 24
            del d['Closed']
            return d


def prepare_list_for_csv(issues):
    return list(filter(lambda x: x is not None,
                       map(collect_dict_from_issue, issues)))


def write_csv_from_list_of_dicts(lst, project_name):
    df = pd.DataFrame.from_records(lst)
    filename = '{}_jira_closed_issues_{}.csv'.format(project_name, time.time())
    df.to_csv(filename, encoding='utf-8', index=False)
    return filename


def create_dr_project(filename, project_name):
    project = Project.start(
        name='{}_jira_closed_issues_{}'.format(
            project_name,
            datetime.datetime.now().ctime()),
        filepath=filename,
        worker_count=8,
        target='Spendtime')
    return project.id


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run jira pull and start project on datarobot')
    parser.add_argument(
        '--jira',
        default='https://issues.apache.org/jira',
        help='host url of endpoint of jira board')
    parser.add_argument(
        '--uname',
        default=None,
        help='username of user')
    parser.add_argument(
        '--pwd',
        default=None,
        help='password of user')
    parser.add_argument(
        '--p_name',
        default='SPARK',
        help='Project name')
    parser.add_argument(
        '--limit',
        default=None,
        help='Limit issues')
    args = parser.parse_args()
    print(args)
    issues = get_closed_issues(jira_link=args.jira,
                               project_name=args.p_name,
                               auth=(args.uname, args.pwd),
                               limit=int(args.limit) if args.limit else None)
    csv_list = prepare_list_for_csv(issues)
    filename = write_csv_from_list_of_dicts(csv_list, project_name=args.p_name)
    create_dr_project(filename, filename)
