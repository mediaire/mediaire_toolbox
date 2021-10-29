#!/bin/env python3
"""
Check that all commits between `prev_release` and `new_release` have an
associated Epic with label `change_request`.

Install dependencies: `python3 -m pip install GitPython jira pyYAML`.

CSV can be imported into GSheets/Excel.

HTML contains links to JIRA and GitLab pages.
"""

import argparse
import sys
import os
import re
import importlib.util
from collections import defaultdict

from git import Repo
from git.exc import GitError
from jira import JIRA
from jira.exceptions import JIRAError
import yaml


# TODO to cli args?
TICKET_NAME_PATTERN = r'[A-Z]{2,}-\d+'
IGNORE_COMMIT_PATTERN = r'Version \d+\.\d+\.\d+, automatic version bump'

JIRA_URL = 'https://mediaire.atlassian.net'
JIRA_FIELD_EPIC_LINK = 'customfield_10014'
JIRA_REQUIRED_LABEL = 'change_request'


def GITLAB_URL(component):
    GITLAB_PROJECT_STUBS = {
        # MDSUITE_COMPONENTS
        # 'anonymizer',
        # 'dicom_grazer',
        # 'dicom_sender',
        # 'dicom_server',
        # 'md_platform',
        'suite_coordinator': 'dev-squad/suite_coordinator',
        # MDBRAIN_COMPONENTS
        'brain_segmentation': 'ml-squad/brain_segmentation',
        # 'lesion_assessment',
        # 'lesion_segmentation',
        # 'longitudinal_assessment',
        # 'longitudinal_grazer',
        # 'report_worker',
        # 'task_manager',
        # 'volumetry_assessment',
        'aneurysm_segmentation': 'ml-squad/aneurysm_segmentation',
        'tumor_segmentation': 'ml-squad/tumor_segmentation',
        # MDKNEE_COMPONENTS
        'knee_task_manager': 'mdknee/knee_task_manager',
        'mdknee_classifier': 'ml-squad/mdknee_classifier',
        # MDSPINE_COMPONENTS
        'spine_lesion_segmentation': 'md.spine/spine_lesion_segmentation',
        'spine_report_worker': 'md.spine/spine_report_worker',
        'spine_task_manager': 'md.spine/spine_task_manager',
    }
    component = GITLAB_PROJECT_STUBS.get(component, component)
    return f'https://gitlab.com/mediaire/{component}'


def print_error(*args, **kwags):
    print(*args, **kwargs, file=sys.stderr)


def print_header(fmt, /, prev_release, new_release):
    if fmt == 'plain':
        pass
    elif fmt == 'csv':
        print('"component","prev_version","new_version","commit","ticket","epic","msg"')
    elif fmt == 'html':
        print(f"""\
<!doctype html>
<html>
<head>
  <title>Missing Change Requests for {prev_release} → {new_release}</title>
  <style>
    body {{
      max-width: 1400px;
      margin: 0 auto;
    }}
    table {{
      font-family: sans-serif;
      margin: 10px auto;
      border-spacing: 0px 0px;
      border-top: 2px solid black;
      border-bottom: 2px solid black;
      border-collapse: collapse;
    }}
    td, th {{ 
      padding: 0.6ex 5px 0.6ex 5px; 
      text-align: left;
    }}
    tr:nth-child(2n) {{
      background: #eee;
    }}
    tr:hover {{
      background: #ddd;
    }}
    thead {{
      border-bottom: 1px solid black;
    }}
    a:link, a:visited {{
      color: black;
      text-decoration: none;
    }}
  </style>
</head>
<body>
<table>
  <thead>
    <tr>
      <th>Component</th>
      <th>Previous Version</th>
      <th>New Version</th>
      <th>Commit</th>
      <th>Ticket</th>
      <th>Epic</th>
      <th>Message</th>
    <tr>
  </thead>
""")
    else:
        raise RuntimeError("Infalid format")


def print_section(fmt, /, component, prev_version, new_version):
    if fmt == 'plain':
        print()
        print(f"{component}: {prev_version} … {new_version}")
    elif fmt == 'csv':
        pass
    elif fmt == 'html':
        pass
    else:
        raise RuntimeError("Infalid format")


def print_row(fmt, /, commit, ticket=None, epic=None, msg='', component=None, prev_version=None, new_version=None):
    if fmt == 'plain':
        ticket = ticket if ticket else '<no ticket>'
        epic = f'Epic:{epic}:"{epic.fields.summary}"' if epic else '<no epic>'
        msg = f"[{msg}]" if msg else ''
        row = f"  {commit}  {ticket}  {epic}  {msg}"
    elif fmt == 'csv':
        component = component if component else ''
        prev_version = prev_version if prev_version else ''
        new_version = new_version if new_version else ''
        ticket = ticket if ticket else ''
        epic = epic if epic else ''
        msg = msg.replace('"', '""')
        row = f'"{component}","{prev_version}","{new_version}","{commit}","{ticket}","{epic}","{msg}"'
    elif fmt == 'html':
        component = component if component else ''
        prev_version = prev_version if prev_version else ''
        new_version = new_version if new_version else ''
        ticket = ticket if ticket else ''
        epic = epic if epic else ''
        row = f"""\
  <tr>
    <td><a href="{GITLAB_URL(component)}">{component}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/tags/{prev_version}">{prev_version}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/tags/{new_version}">{new_version}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/commit/{commit}"><code>{commit}</code></a></td>
    <td><a href="{JIRA_URL}/browse/{ticket}">{ticket}</a></td>
    <td><a href="{JIRA_URL}/browse/{ticket}">{epic}</a></td>
    <td>{msg}</td>
  </tr>\
"""
    else:
        raise RuntimeError("Infalid format")
    print(row)


def print_footer(fmt):
    if fmt == 'plain':
        pass
    elif fmt == 'csv':
        pass
    elif fmt == 'html':
        print("</table>\n</body>\n</html>", end="")
    else:
        raise RuntimeError("Infalid format")


def get_certified_components(mdbrain_manager: Repo) -> set:
    """Get set of certified components."""
    # We load the `generate_docker_compose.py` file from the git blob and
    # remove all import statements. Then we import the module into this Python
    # context. This way we can access the constants defined in that module.
    # Because the executable part is protected by a __name__-guard, we don't
    # actually generate a docker-compose.yml file.
    # TODO constant
    gen_docker_compose_blob = \
        (mdbrain_manager.head.commit.tree
         / 'opt' / 'mdbrain' / 'generate_docker_compose.py')
    gen_docker_compose_lines = \
        gen_docker_compose_blob.data_stream.read().decode('utf-8').split('\n')
    gen_docker_compose_py = '\n'.join(
        filter(lambda l: (not l.startswith('import')
                          and not l.startswith('from')),
               gen_docker_compose_lines)
    )
    import_spec = importlib.util.spec_from_loader(
        'gen_docker_compose',
        loader=None,
        origin='mdbrain_manager.opt.mdbrain.generate_docker_compose'
    )
    gen_docker_compose = importlib.util.module_from_spec(import_spec)
    exec(gen_docker_compose_py, gen_docker_compose.__dict__)

    # TODO make this dynamical via a constant
    # cert = map(union, set(getattr(gen_docker_compose, l) for l in COMP_LISTS)
    non_certified_components = set(gen_docker_compose.NON_CERTIFIED_COMPONENTS)
    mdsuite_components = set(gen_docker_compose.MDSUITE_COMPONENTS)
    mdbrain_components = set(gen_docker_compose.MDBRAIN_COMPONENTS)
    mdspine_components = set(gen_docker_compose.MDSPINE_COMPONENTS)
    certified_components = (mdsuite_components
                            | mdbrain_components
                            | mdspine_components) - non_certified_components
    return certified_components


def get_component_versions(mdbrain_manager: Repo, version: str):
    """Get {component: version} dict for mdbrain version :param:`version`."""
    try:
        tag = next(filter(lambda tag: str(tag).endswith(version),
                          mdbrain_manager.tags))
    except StopIteration:
        raise RuntimeError(f"Tag {version} not in {mdbrain_manager}")
    # TODO constant
    components_yaml = (tag.commit.tree
                       / 'etc' / 'mdbrain' / 'components.yml').data_stream
    return {component: version_dict['version']
            for component, version_dict
            in yaml.safe_load(components_yaml.read())['components'].items()}


# TODO refactor
# split into funtions
# if __name__ == '__main__'


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('prev_release', type=str,
                    help="Version number of previous release.")
parser.add_argument('new_release', type=str,
                    help="Version number of new release.")
parser.add_argument('-g', '--git-root', default=os.getenv('HOME'),
                    help="Path to the directory where all components git"
                         " repoositories reside. Defaults to $HOME.")
parser.add_argument('-f', '--format', type=str,
                    choices=('csv', 'html', 'plain'), default='plain',
                    help="Output format.")
# TODO enable ~/.config/jira-python
parser.add_argument(
    '-j', '--jira-auth', nargs=2, metavar=('JIRA_EMAIL', 'JIRA_TOKEN'),
    default=(os.getenv('JIRA_EMAIL'), os.getenv('JIRA_TOKEN')),
    help="JIRA API authentication. Create a token at"
         " https://id.atlassian.com/manage-profile/security/api-tokens."
         " Can also be set using environment variables by the same name."
)
args = parser.parse_args()


# TODO constant
mdbrain_manager = Repo(os.path.join(args.git_root, 'mdbrain_manager'))
mdbrain_manager.remotes.origin.fetch()

prev_versions = get_component_versions(mdbrain_manager, args.prev_release)
new_versions = get_component_versions(mdbrain_manager, args.new_release)

try:
    jira = JIRA(JIRA_URL, basic_auth=args.jira_auth)
except JIRAError as e:
    print_error("Authenticaiton Error:", e.text)
    sys.exit(1)

print_header(args.format,
             prev_release=args.prev_release,
             new_release=args.new_release)

# used for both epics and other tickets
has_change_request_label = {}

for component in sorted(get_certified_components(mdbrain_manager)):
    try:
        prev_version = prev_versions[component]
        new_version = new_versions[component]
    except KeyError:
        print_error(
            f'! Component "{component}" not specified in "components.yml"')
        continue
    print_section(args.format,
                  component=component,
                  prev_version=prev_version,
                  new_version=new_version)

    try:
        repo = Repo(os.path.join(args.git_root, component))
        repo.remotes.origin.fetch()
    except GitError as e:
        print_error('! git error:', e)
        continue

    if prev_version not in repo.tags or new_version not in repo.tags:
        print_error(
            f'! tag "{prev_version}" or "{new_version}" not found in {repo}')
        continue

    for commit in repo.iter_commits(f'{prev_version}...{new_version}'):
        tickets_in_commit_message = re.findall(TICKET_NAME_PATTERN,
                                               commit.message)
        if tickets_in_commit_message:
            # print(' ', commit.hexsha, *set(matches))
            for ticket_id in set(tickets_in_commit_message):
                if has_change_request_label.get(ticket_id):
                    continue

                try:
                    ticket = jira.issue(ticket_id)

                    epic_id = getattr(ticket.fields, JIRA_FIELD_EPIC_LINK)
                    if epic_id is None:
                        raise AttributeError
                    elif has_change_request_label.get(epic_id):
                        continue

                    epic = jira.issue(epic_id)
                    if JIRA_REQUIRED_LABEL not in epic.fields.labels:
                        raise ValueError
                    else:
                        has_change_request_label[ticket_id] = True
                        has_change_request_label[epic_id] = True
                except AttributeError:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              ticket=ticket_id,
                              epic=None,
                              msg='')
                except ValueError:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              ticket=ticket_id,
                              epic=epic,
                              msg=f'epic has no label "{JIRA_REQUIRED_LABEL}"')
                except JIRAError as e:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              ticket=ticket_id,
                              epic=None,
                              msg=f'JIRA error: "{e.text}"')
        else:
            commit_subject = commit.message.split('\n')[0]
            if re.match(IGNORE_COMMIT_PATTERN, commit_subject):
                continue
            print_row(args.format,
                      component=component,
                      prev_version=prev_version,
                      new_version=new_version,
                      commit=commit.hexsha,
                      ticket=None,
                      epic=None,
                      msg=f'{commit_subject}')

print_footer(args.format)
