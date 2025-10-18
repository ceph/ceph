from botocore.exceptions import ClientError
import pytest

from . import (
    configfile,
    get_iam_root_client,
    get_iam_root_user_id,
    get_iam_root_email,
    get_iam_alt_root_client,
    get_iam_alt_root_user_id,
    get_iam_alt_root_email,
    get_iam_path_prefix,
)

def nuke_user_keys(client, name):
    p = client.get_paginator('list_access_keys')
    for response in p.paginate(UserName=name):
        for key in response['AccessKeyMetadata']:
            try:
                client.delete_access_key(UserName=name, AccessKeyId=key['AccessKeyId'])
            except:
                pass

def nuke_user_policies(client, name):
    p = client.get_paginator('list_user_policies')
    for response in p.paginate(UserName=name):
        for policy in response['PolicyNames']:
            try:
                client.delete_user_policy(UserName=name, PolicyName=policy)
            except:
                pass

def nuke_attached_user_policies(client, name):
    p = client.get_paginator('list_attached_user_policies')
    for response in p.paginate(UserName=name):
        for policy in response['AttachedPolicies']:
            try:
                client.detach_user_policy(UserName=name, PolicyArn=policy['PolicyArn'])
            except:
                pass

def nuke_user(client, name):
    # delete access keys, user policies, etc
    try:
        nuke_user_keys(client, name)
    except:
        pass
    try:
        nuke_user_policies(client, name)
    except:
        pass
    try:
        nuke_attached_user_policies(client, name)
    except:
        pass
    client.delete_user(UserName=name)

def nuke_users(client, **kwargs):
    p = client.get_paginator('list_users')
    for response in p.paginate(**kwargs):
        for user in response['Users']:
            try:
                nuke_user(client, user['UserName'])
            except:
                pass

def nuke_group_policies(client, name):
    p = client.get_paginator('list_group_policies')
    for response in p.paginate(GroupName=name):
        for policy in response['PolicyNames']:
            try:
                client.delete_group_policy(GroupName=name, PolicyName=policy)
            except:
                pass

def nuke_attached_group_policies(client, name):
    p = client.get_paginator('list_attached_group_policies')
    for response in p.paginate(GroupName=name):
        for policy in response['AttachedPolicies']:
            try:
                client.detach_group_policy(GroupName=name, PolicyArn=policy['PolicyArn'])
            except:
                pass

def nuke_group_users(client, name):
    p = client.get_paginator('get_group')
    for response in p.paginate(GroupName=name):
        for user in response['Users']:
            try:
                client.remove_user_from_group(GroupName=name, UserName=user['UserName'])
            except:
                pass

def nuke_group(client, name):
    # delete group policies and remove all users
    try:
        nuke_group_policies(client, name)
    except:
        pass
    try:
        nuke_attached_group_policies(client, name)
    except:
        pass
    try:
        nuke_group_users(client, name)
    except:
        pass
    client.delete_group(GroupName=name)

def nuke_groups(client, **kwargs):
    p = client.get_paginator('list_groups')
    for response in p.paginate(**kwargs):
        for user in response['Groups']:
            try:
                nuke_group(client, user['GroupName'])
            except:
                pass

def nuke_role_policies(client, name):
    p = client.get_paginator('list_role_policies')
    for response in p.paginate(RoleName=name):
        for policy in response['PolicyNames']:
            try:
                client.delete_role_policy(RoleName=name, PolicyName=policy)
            except:
                pass

def nuke_attached_role_policies(client, name):
    p = client.get_paginator('list_attached_role_policies')
    for response in p.paginate(RoleName=name):
        for policy in response['AttachedPolicies']:
            try:
                client.detach_role_policy(RoleName=name, PolicyArn=policy['PolicyArn'])
            except:
                pass

def nuke_role(client, name):
    # delete role policies, etc
    try:
        nuke_role_policies(client, name)
    except:
        pass
    try:
        nuke_attached_role_policies(client, name)
    except:
        pass
    client.delete_role(RoleName=name)

def nuke_roles(client, **kwargs):
    p = client.get_paginator('list_roles')
    for response in p.paginate(**kwargs):
        for role in response['Roles']:
            try:
                nuke_role(client, role['RoleName'])
            except:
                pass

def nuke_oidc_providers(client, prefix):
    result = client.list_open_id_connect_providers()
    for provider in result['OpenIDConnectProviderList']:
        arn = provider['Arn']
        if f':oidc-provider{prefix}' in arn:
            try:
                client.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            except:
                pass


# fixture for iam account root user
@pytest.fixture
def iam_root(configfile):
    client = get_iam_root_client()
    try:
        arn = client.get_user()['User']['Arn']
        if not arn.endswith(':root'):
            pytest.skip('[iam root] user does not have :root arn')
    except ClientError as e:
        pytest.skip('[iam root] user does not belong to an account')

    yield client
    nuke_users(client, PathPrefix=get_iam_path_prefix())
    nuke_groups(client, PathPrefix=get_iam_path_prefix())
    nuke_roles(client, PathPrefix=get_iam_path_prefix())
    nuke_oidc_providers(client, get_iam_path_prefix())

# fixture for iam alt account root user
@pytest.fixture
def iam_alt_root(configfile):
    client = get_iam_alt_root_client()
    try:
        arn = client.get_user()['User']['Arn']
        if not arn.endswith(':root'):
            pytest.skip('[iam alt root] user does not have :root arn')
    except ClientError as e:
        pytest.skip('[iam alt root] user does not belong to an account')

    yield client
    nuke_users(client, PathPrefix=get_iam_path_prefix())
    nuke_roles(client, PathPrefix=get_iam_path_prefix())
