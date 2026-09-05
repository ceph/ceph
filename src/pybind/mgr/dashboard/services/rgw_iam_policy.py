from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..exceptions import DashboardException

_POLICY_VERSION_LIMIT = 5


class RgwIamPolicies:
    """
    Customer-managed IAM policies for RGW accounts.

    RGW does not yet implement the IAM CreatePolicy/ListPolicies APIs. Until
    that is available, policies are stored in the dashboard mgr module.
    """

    _policies: Dict[str, List[Dict[str, Any]]] = {}
    _versions: Dict[str, List[Dict[str, Any]]] = {}
    _tags: Dict[str, List[Dict[str, str]]] = {}

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    @classmethod
    def _build_policy_arn(cls, account_id: str, path: str, policy_name: str) -> str:
        policy_path = (path or '/').rstrip('/')
        return f'arn:aws:iam::{account_id}:policy{policy_path}/{policy_name}'

    @classmethod
    def _find_policy(cls, policy_arn: str) -> Optional[Dict[str, Any]]:
        for policies in cls._policies.values():
            for policy in policies:
                if policy.get('Arn') == policy_arn:
                    return policy
        return None

    @classmethod
    def _policy_versions(cls, policy_arn: str) -> List[Dict[str, Any]]:
        if policy_arn not in cls._versions:
            cls._versions[policy_arn] = []
        return cls._versions[policy_arn]

    @classmethod
    def _policy_tags(cls, policy_arn: str) -> List[Dict[str, str]]:
        if policy_arn not in cls._tags:
            cls._tags[policy_arn] = []
        return cls._tags[policy_arn]

    @classmethod
    def _init_default_version(cls, policy_arn: str, policy_doc: str,
                              create_date: Optional[str] = None):
        versions = cls._policy_versions(policy_arn)
        if versions:
            return
        versions.append({
            'VersionId': 'v1',
            'IsDefaultVersion': True,
            'CreateDate': create_date or cls._utc_now(),
            'Document': policy_doc,
        })

    @classmethod
    def _next_version_id(cls, policy_arn: str) -> str:
        max_version = 0
        for version in cls._policy_versions(policy_arn):
            version_id = version.get('VersionId', '')
            if version_id.startswith('v') and version_id[1:].isdigit():
                max_version = max(max_version, int(version_id[1:]))
        return f'v{max_version + 1}'

    @classmethod
    def _remove_policy(cls, policy_arn: str):
        for account_id, policies in cls._policies.items():
            cls._policies[account_id] = [
                policy for policy in policies if policy.get('Arn') != policy_arn
            ]
        cls._versions.pop(policy_arn, None)
        cls._tags.pop(policy_arn, None)

    @classmethod
    def _require_policy(cls, policy_arn: str) -> Dict[str, Any]:
        policy = cls._find_policy(policy_arn)
        if not policy:
            raise DashboardException(msg='Policy not found',
                                     http_status_code=404, component='rgw')
        return policy

    @classmethod
    def list_policies(cls, account_id: str):
        if not account_id:
            return []
        return list(cls._policies.get(account_id, []))

    @classmethod
    def get_policy(cls, policy_arn: str):
        return cls._find_policy(policy_arn) or {}

    @classmethod
    def create_policy(cls, policy_name: str, policy_doc: str, account_id: str,
                      path: str = '/', description: str = ''):
        if not account_id:
            raise DashboardException(msg='account_id is required',
                                     http_status_code=400, component='rgw')

        arn = cls._build_policy_arn(account_id, path, policy_name)
        create_date = cls._utc_now()
        policy = {
            'PolicyName': policy_name,
            'Arn': arn,
            'Path': path or '/',
            'DefaultVersionId': 'v1',
            'CreateDate': create_date,
            'Description': description or '',
            'PolicyDocument': policy_doc,
        }

        account_policies = cls._policies.setdefault(account_id, [])
        account_policies[:] = [item for item in account_policies if item.get('Arn') != arn]
        account_policies.append(policy)
        cls._init_default_version(arn, policy_doc, create_date)
        return policy

    @classmethod
    def delete_policy(cls, policy_arn: str):
        cls._remove_policy(policy_arn)
        return {}

    @classmethod
    def delete_policy_by_name(cls, account_id: str, policy_name: str):
        for policy in cls._policies.get(account_id, []):
            if policy.get('PolicyName') == policy_name:
                return cls.delete_policy(policy['Arn'])
        raise DashboardException(msg='Policy not found',
                                 http_status_code=404, component='rgw')

    @classmethod
    def list_policy_versions(cls, policy_arn: str):
        policy = cls._find_policy(policy_arn)
        if policy:
            cls._init_default_version(
                policy_arn,
                policy.get('PolicyDocument', '{}'),
                policy.get('CreateDate')
            )
        return list(cls._policy_versions(policy_arn))

    @classmethod
    def get_policy_version(cls, policy_arn: str, version_id: str):
        for version in cls._policy_versions(policy_arn):
            if version.get('VersionId') == version_id:
                return dict(version)
        return {}

    @classmethod
    def create_policy_version(cls, policy_arn: str, policy_doc: str,
                              set_as_default: bool = False):
        cls._require_policy(policy_arn)
        versions = cls._policy_versions(policy_arn)
        if not versions:
            cls._init_default_version(policy_arn, policy_doc)
            versions = cls._policy_versions(policy_arn)

        if len(versions) >= _POLICY_VERSION_LIMIT:
            raise DashboardException(
                msg=f'A policy cannot have more than {_POLICY_VERSION_LIMIT} versions',
                http_status_code=409,
                component='rgw')

        version_id = cls._next_version_id(policy_arn)
        new_version = {
            'VersionId': version_id,
            'IsDefaultVersion': set_as_default,
            'CreateDate': cls._utc_now(),
            'Document': policy_doc,
        }
        if set_as_default:
            for version in versions:
                version['IsDefaultVersion'] = False
            policy = cls._find_policy(policy_arn)
            if policy:
                policy['DefaultVersionId'] = version_id
                policy['PolicyDocument'] = policy_doc
        versions.append(new_version)
        return new_version

    @classmethod
    def delete_policy_version(cls, policy_arn: str, version_id: str):
        versions = cls._policy_versions(policy_arn)
        version = next((item for item in versions if item.get('VersionId') == version_id), None)
        if not version:
            raise DashboardException(msg='Policy version not found',
                                     http_status_code=404, component='rgw')
        if version.get('IsDefaultVersion') in (True, 'true'):
            raise DashboardException(msg='Cannot delete the default policy version',
                                     http_status_code=409, component='rgw')

        cls._versions[policy_arn] = [
            item for item in versions if item.get('VersionId') != version_id
        ]
        return {}

    @classmethod
    def set_default_policy_version(cls, policy_arn: str, version_id: str):
        versions = cls._policy_versions(policy_arn)
        version = next((item for item in versions if item.get('VersionId') == version_id), None)
        if not version:
            raise DashboardException(msg='Policy version not found',
                                     http_status_code=404, component='rgw')

        for item in versions:
            item['IsDefaultVersion'] = item.get('VersionId') == version_id
        policy = cls._find_policy(policy_arn)
        if policy:
            policy['DefaultVersionId'] = version_id
            policy['PolicyDocument'] = version.get('Document', '')
        return {}

    @classmethod
    def list_policy_tags(cls, policy_arn: str):
        return list(cls._policy_tags(policy_arn))

    @classmethod
    def tag_policy(cls, policy_arn: str, tags: List[Dict[str, str]]):
        if not tags:
            raise DashboardException(msg='At least one tag is required',
                                     http_status_code=400, component='rgw')
        cls._require_policy(policy_arn)

        tag_list = cls._policy_tags(policy_arn)
        for tag in tags:
            tag_list[:] = [item for item in tag_list if item.get('Key') != tag['Key']]
            tag_list.append({'Key': tag['Key'], 'Value': tag['Value']})
        return {}

    @classmethod
    def untag_policy(cls, policy_arn: str, tag_keys: List[str]):
        if not tag_keys:
            raise DashboardException(msg='At least one tag key is required',
                                     http_status_code=400, component='rgw')
        cls._require_policy(policy_arn)

        tag_list = cls._policy_tags(policy_arn)
        cls._tags[policy_arn] = [
            tag for tag in tag_list if tag.get('Key') not in tag_keys
        ]
        return {}
