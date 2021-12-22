#!/bin/env python3
"""
Check that all commits between `prev_release` and `new_release` have an
associated Epic with label `change_request`.

Install dependencies:
`python3 -m pip install GitPython jira pyYAML python-gitlab`.

CSV can be imported into GSheets/Excel.

HTML contains links to JIRA and GitLab pages.
"""

# TODO
# - refactor to always check a list of possible epics, keep a list of error
#   messages and output all of them if no change_request epic is found among
#   them make list from
#   - self
#   - JIRA_FIELD_EPIC_LINK
#   - parent
#   - blocked_linked_epics
# - add ticket title to the output so it's easier to guess which change_request
#   epic it shoudl be assoc'd with
# - add js funcitonality to quickly apply a change_request epic block link
#   - use jira api with js
#   - pass jira token via url anchor, output in commandline
#   - automatically get all change_request epics from the
#     mdbrain-core/change_requests board
#   - if that's too complicated, just output tsv list of epics to paste into
#     jira maually
#   - another way: use python and output cli call in html
#   - use iframe to show jira ticket page and crop to issue links
#     (https://stackoverflow.com/a/5676721/894166)
# - group by tickets
#   - UI for commits without tickets
# - just go HTML only

import argparse
import sys
import os
import re
import importlib.util

import yaml
from git import Repo
from git.exc import GitError
from jira import JIRA
from jira.exceptions import JIRAError
from gitlab import Gitlab


# TODO to cli args?
TICKET_NAME_PATTERN = r'[A-Z]{2,}-\d+'
IGNORE_COMMIT_PATTERN = r'Version \d+\.\d+\.\d+, automatic version bump'

JIRA_URL = 'https://mediaire.atlassian.net'
JIRA_FIELD_EPIC_LINK = 'customfield_10014'
JIRA_REQUIRED_LABEL = 'change_request'

GITLAB_HOST = 'https://gitlab.com'

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


def GITLAB_URL(component):
    component = GITLAB_PROJECT_STUBS.get(component, component)
    return f'{GITLAB_HOST}/mediaire/{component}'


def print_error(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)


def print_header(fmt, /, prev_release, new_release):
    if fmt == 'plain':
        pass
    elif fmt == 'csv':
        print('"component",'
              '"prev_version",'
              '"new_version",'
              '"commit",'
              '"merge_request",'
              '"ticket",'
              '"epic",'
              '"msg"')
    elif fmt == 'html':
        print(f"""\
<!doctype html>
<html>
<head>
  <meta charset="UTF-8">
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
  <script>
    function archive(elem) {{
      td = elem.parentNode;
      tr = td.parentNode;
      if (elem.innerText === "done") {{
        tr.style.opacity = "0.1";
        elem.innerText = "todo";
      }} else {{
        tr.style.opacity = "1";
        elem.innerText = "done";
      }}
    }}
  </script>
</head>
<body>
<table>
  <thead>
    <tr>
      <th>Component</th>
      <th>Previous Version</th>
      <th>New Version</th>
      <th>Commit</th>
      <th>Merge Request</th>
      <th>Ticket</th>
      <th>Epic</th>
      <th>Message</th>
      <th>Archive</th>
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


def print_row(fmt,
              /,
              commit,
              ticket=None,
              epic=None,
              msg='',
              component=None,
              prev_version=None,
              new_version=None,
              merge_request=None):
    if fmt == 'plain':
        ticket = ticket if ticket else '<no ticket>'
        epic = f'Epic:{epic}:"{epic.fields.summary}"' if epic else '<no epic>'
        merge_request = (f'!{merge_request["iid"]}'
                         if merge_request
                         else '<no MR>')
        msg = f"[{msg}]" if msg else ''
        row = f"  {commit}  {merge_request}  {ticket}  {epic}  {msg}"
    elif fmt == 'csv':
        component = component if component else ''
        prev_version = prev_version if prev_version else ''
        new_version = new_version if new_version else ''
        ticket = ticket if ticket else ''
        epic = epic if epic else ''
        merge_request = merge_request["iid"] if merge_request else ''
        msg = msg.replace('"', '""')
        row = (f'"{component}",'
               f'"{prev_version}",'
               f'"{new_version}",'
               f'"{commit}",'
               f'"{merge_request}",'
               f'"{ticket}",'
               f'"{epic}","{msg}"')
    elif fmt == 'html':
        component = component if component else ''
        prev_version = prev_version if prev_version else ''
        new_version = new_version if new_version else ''
        merge_request = merge_request['iid'] if merge_request else ''
        ticket = ticket if ticket else ''
        epic = epic if epic else ''
        row = f"""\
  <tr>
    <td><a href="{GITLAB_URL(component)}">{component}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/tags/{prev_version}">{prev_version}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/tags/{new_version}">{new_version}</a></td>
    <td><a href="{GITLAB_URL(component)}/-/commit/{commit}"><code>{commit}</code></a></td>
    <td><a href="{GITLAB_URL(component)}/-/merge_requests/{merge_request}">!{merge_request}</a></td>
    <td><a href="{JIRA_URL}/browse/{ticket}">{ticket}</a></td>
    <td><a href="{JIRA_URL}/browse/{epic}">{epic}</a></td>
    <td>{msg}</td>
    <td><button onclick="archive(this);">done</button></</td>
  </tr>\
"""  # noqa: E501
    else:
        raise RuntimeError("Infalid format")
    print(row)


def print_footer(fmt):
    if fmt == 'plain':
        pass
    elif fmt == 'csv':
        pass
    elif fmt == 'html':
        print("</table>\n</body>\n</html>")
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


def get_component_versions_manager(reference_repo: Repo, version: str):
    """Get {component: version} dict for mdbrain version :param:`version`."""
    try:
        ref = next(filter(lambda tag: str(tag).endswith(version),
                          reference_repo.tags))
    except StopIteration:
        print_error(
            f"Tag {version} not in {reference_repo}, using HEAD instead")
        ref = reference_repo.head

    # TODO constant
    components_yaml = (ref.commit.tree
                       / 'etc' / 'mdbrain' / 'components.yml').data_stream
    return {component: version_dict['version']
            for component, version_dict
            in yaml.safe_load(components_yaml.read())['components'].items()}


def get_component_versions_orchestration(reference_repo: Repo, version: str):
    """Get {component: version} dict for mdbrain version :param:`version`."""
    if version == 'dev':
        ref = reference_repo.heads.dev
    else:
        ref = next(filter(lambda tag: str(tag).endswith(version),
                          reference_repo.tags))

    # TODO constant
    components_yaml = (ref.commit.tree
                       / 'mdbrain' / 'config' / 'components.yml').data_stream
    return {component: version_dict['version']
            for component, version_dict
            in yaml.safe_load(components_yaml.read())['components'].items()}


def is_blocked_epic(issue_link) -> bool:
    """Check if issue link outwoard blocks an epic."""
    if not hasattr(issue_link, 'outwardIssue'):
        return False
    return (issue_link.outwardIssue.fields.issuetype.name == 'Epic'
            and issue_link.type.outward == 'blocks')


def get_parent_epic(issue):
    """Get the parent epic of `issue`.

    Different boards have different meta data formats.
    """
    return (
        # DEV board
        getattr(ticket.fields, JIRA_FIELD_EPIC_LINK, None)
        # BS board
        or getattr(getattr(ticket.fields, 'parent', None), 'key', None)
    )

# TODO refactor
# split into funtions
# if __name__ == '__main__'


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('prev_release', type=str,
                    help="Version number of previous release.")
# TODO this will always look into container_orchestration 'dev' head
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
parser.add_argument(
    '-l', '--gitlab-auth', metavar='GITLAB_TOKEN',
    default=os.getenv('GITLAB_TOKEN'),
    help="Gitlab API authentication. Create a private token at"
         " https://gitlab.com/-/profile/personal_access_tokens"
         " Can also be set using environment variables by the same name."
)
args = parser.parse_args()


# TODO constant
# TODO apparently this is not enough to fetch all branches?
mdbrain_manager = Repo(os.path.join(args.git_root, 'mdbrain_manager'))
mdbrain_manager.remotes.origin.fetch()
container_orchestration = Repo(os.path.join(args.git_root,
                                            'container_orchestration'))
container_orchestration.remotes.origin.fetch()

prev_versions = get_component_versions_manager(mdbrain_manager,
                                               args.prev_release)
new_versions = get_component_versions_orchestration(container_orchestration,
                                                    'dev')

try:
    jira = JIRA(JIRA_URL, basic_auth=args.jira_auth)
    gitlab = Gitlab(GITLAB_HOST, private_token=args.gitlab_auth)
except JIRAError as e:  # TODO
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

    if prev_version not in repo.tags:
        print_error(
            f'! tag "{prev_version}" or "{new_version}" not found in {repo}')
        continue
    if new_version not in repo.tags:
        print_error(
            f'! tag "{prev_version}" or "{new_version}" not found in {repo}')
        print_error('Using HEAD instead')
        new_version = 'HEAD'

    try:
        gl_repo = gitlab.projects.get(
            f'mediaire/{GITLAB_PROJECT_STUBS.get(component, component)}')
    except Exception as e:  # TODO
        print_error('! Gitlab error:', e)
        continue

    for commit in repo.iter_commits(f'{prev_version}...{new_version}'):
        gl_commit = gl_repo.commits.get(commit.hexsha)
        associated_merge_requests = gl_commit.merge_requests()
        tickets_in_merge_requests = set(re.findall(
            TICKET_NAME_PATTERN,
            '\n'.join(mr['title'] for mr in associated_merge_requests)
        ))
        merge_request = (associated_merge_requests[0]
                         if associated_merge_requests
                         else None)

        tickets_in_commit_message = set(re.findall(TICKET_NAME_PATTERN,
                                                   commit.message))

        associated_tickets = \
            tickets_in_merge_requests | tickets_in_commit_message
        if associated_tickets:
            # print(' ', commit.hexsha, *set(matches))
            for ticket_id in set(associated_tickets):
                if has_change_request_label.get(ticket_id):
                    continue

                try:
                    ticket = jira.issue(ticket_id)
                    try:

                        epic_id = get_parent_epic(ticket)
                        if epic_id is None:
                            raise AttributeError("has no epic")
                        elif has_change_request_label.get(epic_id):
                            continue

                        epic = jira.issue(epic_id)
                        fix_versions = [fv.name for fv
                                        in epic.fields.fixVersions]
                        if JIRA_REQUIRED_LABEL not in epic.fields.labels:
                            raise ValueError(
                                f'epic has no label "{JIRA_REQUIRED_LABEL}"')
                        elif args.new_release not in fix_versions:
                            raise ValueError(
                                f'epic "{JIRA_REQUIRED_LABEL}"'
                                f' has wrong version: {fix_versions}')
                        else:
                            has_change_request_label[ticket_id] = True
                            has_change_request_label[epic_id] = True
                    except (AttributeError, ValueError) as main_epic_error:
                        # The main epic did not provide a change request label
                        # therefore look for blocked epics in the issue links
                        # and check those
                        has_change_request_label_in_linked_epic = False
                        blocked_linked_epics = \
                            filter(is_blocked_epic, ticket.fields.issuelinks)
                        if not blocked_linked_epics:
                            raise AttributeError(
                                "has no epic and no blocked linked epics.")
                        for link in blocked_linked_epics:
                            epic_id = link.outwardIssue.key
                            epic = jira.issue(epic_id)
                            fix_versions = [fv.name for fv
                                            in epic.fields.fixVersions]
                            if (has_change_request_label.get(epic_id)
                                    or (JIRA_REQUIRED_LABEL
                                            in epic.fields.labels  # noqa: E127
                                        and args.new_release in fix_versions)):
                                has_change_request_label[epic_id] = True
                                has_change_request_label_in_linked_epic = True

                        if not has_change_request_label_in_linked_epic:
                            raise main_epic_error

                except AttributeError as e:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              merge_request=merge_request,
                              ticket=ticket_id,
                              epic=None,
                              msg=e)
                except ValueError as e:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              merge_request=merge_request,
                              ticket=ticket_id,
                              epic=epic,
                              msg=e)
                except JIRAError as e:
                    print_row(args.format,
                              component=component,
                              prev_version=prev_version,
                              new_version=new_version,
                              commit=commit.hexsha,
                              merge_request=merge_request,
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
                      merge_request=merge_request,
                      ticket=None,
                      epic=None,
                      msg=f'{commit_subject}')

print_footer(args.format)
