=============================
 Ceph Object Gateway IAM API
=============================

.. versionadded:: Squid

The Ceph Object Gateway supports a subset of the `Amazon IAM API`_ for
the RESTful management of account users, roles, and associated policies.

This REST API is served by the same HTTP endpoint as the
`Ceph Object Gateway S3 API`_.

Authorization
=============

By default, only :ref:`Account Root Users <radosgw-account-root-user>` are
authorized to use the IAM API, and can only see the resources under their own
account. The account root user can use policies to delegate these permissions
to other users or roles in the account.

Feature Support
===============

The following tables describe the currently supported IAM actions.

Users
-----

+------------------------------+---------------------------------------------+
| Action                       | Remarks                                     |
+==============================+=============================================+
| **CreateUser**               |                                             |
+------------------------------+---------------------------------------------+
| **GetUser**                  |                                             |
+------------------------------+---------------------------------------------+
| **UpdateUser**               |                                             |
+------------------------------+---------------------------------------------+
| **DeleteUser**               |                                             |
+------------------------------+---------------------------------------------+
| **ListUsers**                |                                             |
+------------------------------+---------------------------------------------+
| **CreateAccessKey**          |                                             |
+------------------------------+---------------------------------------------+
| **UpdateAccessKey**          |                                             |
+------------------------------+---------------------------------------------+
| **DeleteAccessKey**          |                                             |
+------------------------------+---------------------------------------------+
| **ListAccessKeys**           |                                             |
+------------------------------+---------------------------------------------+
| **PutUserPolicy**            |                                             |
+------------------------------+---------------------------------------------+
| **GetUserPolicy**            |                                             |
+------------------------------+---------------------------------------------+
| **DeleteUserPolicy**         |                                             |
+------------------------------+---------------------------------------------+
| **ListUserPolicies**         |                                             |
+------------------------------+---------------------------------------------+
| **AttachUserPolicies**       |                                             |
+------------------------------+---------------------------------------------+
| **DetachUserPolicy**         |                                             |
+------------------------------+---------------------------------------------+
| **ListAttachedUserPolicies** |                                             |
+------------------------------+---------------------------------------------+

Groups
------

+-------------------------------+--------------------------------------------+
| Action                        | Remarks                                    |
+===============================+============================================+
| **CreateGroup**               |                                            |
+-------------------------------+--------------------------------------------+
| **GetGroup**                  |                                            |
+-------------------------------+--------------------------------------------+
| **UpdateGroup**               |                                            |
+-------------------------------+--------------------------------------------+
| **DeleteGroup**               |                                            |
+-------------------------------+--------------------------------------------+
| **ListGroups**                |                                            |
+-------------------------------+--------------------------------------------+
| **AddUserToGroup**            |                                            |
+-------------------------------+--------------------------------------------+
| **RemoveUserFromGroup**       |                                            |
+-------------------------------+--------------------------------------------+
| **ListGroupsForUser**         |                                            |
+-------------------------------+--------------------------------------------+
| **PutGroupPolicy**            |                                            |
+-------------------------------+--------------------------------------------+
| **GetGroupPolicy**            |                                            |
+-------------------------------+--------------------------------------------+
| **DeleteGroupPolicy**         |                                            |
+-------------------------------+--------------------------------------------+
| **ListGroupPolicies**         |                                            |
+-------------------------------+--------------------------------------------+
| **AttachGroupPolicies**       |                                            |
+-------------------------------+--------------------------------------------+
| **DetachGroupPolicy**         |                                            |
+-------------------------------+--------------------------------------------+
| **ListAttachedGroupPolicies** |                                            |
+-------------------------------+--------------------------------------------+

Roles
-----

+------------------------------+---------------------------------------------+
| Action                       | Remarks                                     |
+==============================+=============================================+
| **CreateRole**               |                                             |
+------------------------------+---------------------------------------------+
| **GetRole**                  |                                             |
+------------------------------+---------------------------------------------+
| **UpdateRole**               |                                             |
+------------------------------+---------------------------------------------+
| **UpdateAssumeRolePolicy**   |                                             |
+------------------------------+---------------------------------------------+
| **DeleteRole**               |                                             |
+------------------------------+---------------------------------------------+
| **ListRoles**                |                                             |
+------------------------------+---------------------------------------------+
| **TagRole**                  |                                             |
+------------------------------+---------------------------------------------+
| **UntagRole**                |                                             |
+------------------------------+---------------------------------------------+
| **ListRoleTags**             |                                             |
+------------------------------+---------------------------------------------+
| **PutRolePolicy**            |                                             |
+------------------------------+---------------------------------------------+
| **GetRolePolicy**            |                                             |
+------------------------------+---------------------------------------------+
| **DeleteRolePolicy**         |                                             |
+------------------------------+---------------------------------------------+
| **ListRolePolicies**         |                                             |
+------------------------------+---------------------------------------------+
| **AttachRolePolicies**       |                                             |
+------------------------------+---------------------------------------------+
| **DetachRolePolicy**         |                                             |
+------------------------------+---------------------------------------------+
| **ListAttachedRolePolicies** |                                             |
+------------------------------+---------------------------------------------+

OpenIDConnectProvider
---------------------

+---------------------------------+------------------------------------------+
| Action                          | Remarks                                  |
+=================================+==========================================+
| **CreateOpenIDConnectProvider** |                                          |
+---------------------------------+------------------------------------------+
| **GetOpenIDConnectProvider**    |                                          |
+---------------------------------+------------------------------------------+
| **DeleteOpenIDConnectProvider** |                                          |
+---------------------------------+------------------------------------------+
| **ListOpenIDConnectProviders**  |                                          |
+---------------------------------+------------------------------------------+

Managed Policies
----------------

The following managed policies are available for use with ``AttachGroupPolicy``,
``AttachRolePolicy`` and ``AttachUserPolicy``:

IAMFullAccess
	:Arn: ``arn:aws:iam::aws:policy/IAMFullAccess``
	:Version: v2 (default)

IAMReadOnlyAccess
	:Arn: ``arn:aws:iam::aws:policy/IAMReadOnlyAccess``
	:Version: v4 (default)

AmazonSNSFullAccess
	:Arn: ``arn:aws:iam::aws:policy/AmazonSNSFullAccess``
	:Version: v1 (default)

AmazonSNSReadOnlyAccess
	:Arn: ``arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess``
	:Version: v1 (default)

AmazonS3FullAccess
	:Arn: ``arn:aws:iam::aws:policy/AmazonS3FullAccess``
	:Version: v2 (default)

AmazonS3ReadOnlyAccess
	:Arn: ``arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess``
	:Version: v3 (default)


.. _Amazon IAM API: https://docs.aws.amazon.com/IAM/latest/APIReference/welcome.html
.. _Ceph Object Gateway S3 API: ../s3/

Customer Managed Policies
-------------------------
High level design of Customer Managed Policies in RGW


A managed policy is a standalone policy - that is not inline with any Principal entity. These policies can be attached to any entity (user, role, group etc) and are evaluated along with other IAM policies during permission evaluation for any request coming into RGW. Please refer to https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_managed-vs-inline.html#customer-managed-policies.


A customer managed policy will be stored in a Rados Object of the name accounts.<policy-id>, in rgw metadata pool. Customer Managed Policies support multiple versions of a policy, with the default version being attached to any principal entity. The following is the structure that will be stored as a Customer Managed Policy:


using VersionId = string; // "v1" etc
using PolicyDocument = string;

enum {
  Pending = 0,
  Committed = 1
}Attached;

struct ManagedPolicy {
  string path;
  string name;
  string description;
  string policy_id;
  uint32_t next_version; //tracks next version
  
  map<VersionId, PolicyDocument> versions;
  VersionId default_version;
  
  map<ARN, ManagedPolicyAttachment> attachments;
};

struct ManagedPolicyAttachment {
  string name; // group/role/user name
  Attached state; // track "pending" vs "committed" for consistency mechanism
  };


There will be another Rados object of type accounts.<policy-name> which contains a mapping of policy name to policy id, and this helps in policy lookup by name. Although other entities have another Rados object that maps the policy path to policy-id, we will not have one for Customer Managed Policies as they will only be allowed to be created under user accounts (and there will be no provision to create one under tenant namespace). Since the customer managed policy will be linked to the accounts metadata, and support for listing by path prefix is already provided for a resource under accounts, we can use the same mechanism for listing customer managed policies by path prefix.
Therefore, for a create request for a Customer Managed Policy, the following will be performed:
write_name - links policy name to policy id in accounts.<policy-name> rados object
write_info - which stores the ManagedPolicy structure in accounts.<policy-id> rados object -
rgw_write_sys_obj - writes it as a system object, which will ensure that it is added to the metadata cache, and the policies will be loaded from it during permission evaluation.
add() - which links Managed Policy to the accounts object
And the policy document will be saved as the default policy.


For any AttachUser/Group/RolePolicy API, the following steps will be performed:
Set the group/role/user name in the ManagedPolicyAttachment structure and set the state to “Pending”, and save this in the “attachments” field of ManagedPolicy.
Set the CustomerManagedPolicy ARN in the role/ user/ group.
Set the state to “Committed” in the ManagedPolicyAttachment.
The above ensures consistency if RGW crashes in any of the steps after 1, then we can recover from this during a GetPolicy (or any other similar API ), by checking if the state is “Pending” in the Managed Policy and the Policy ARN is set in the entity, then the state can be updated in the ManagedPolicy and the entity can be returned as part of the response, else if the ARN has not been set in the entity, then it can be removed from the ManagedPolicy also.


Versioning
The different versions of a policy and the default version will be part of Managed Policy structure, saved in the accounts.<policy-info> rados object. Since the Principal entities always have the default version attached to them, therefore, while reading the policy for an entity, first its default version can be fetched and then the policy corresponding to it can be read and returned.


Permission evaluation
Role - load the customer managed policy corresponding to the ARN for all customer managed policies into iam_identity_policies of req_state
Group & User both use load_managed_policy, which calls get_managed_policy() - add implementation to load customer managed policies


Changes to be made to Role, Group, User
User - re-use policies added as an xattr (RGW_ATTR_MANAGED_POLICY) for RGWUserInfo
Role - re-use existing managed_policies in RGWRoleInfo
Group - re-use policies added as an xattr (RGW_ATTR_MANAGED_POLICY) for RGWGroupInfo


Metadata replication for Customer Managed Policy
Add MetadataHandler class similar to other entities (like Role)


Methods that will be supported:
The following methods have to be implemented:
CreatePolicy - Creates a new customer managed policy
DeletePolicy - Deletes the specified managed policy
GetPolicy - Retrieves information about the specified managed policy
ListPolicies - Lists all managed policies
CreatePolicyVersion - Creates a new version of the specified managed policy
DeletePolicyVersion - Deletes a version of the specified managed policy
GetPolicyVersion - Retrieves information about a specific version of a managed policy
ListPolicyVersions - Lists information about the versions of the specified managed policy
SetDefaultPolicyVersion - Sets the specified version as the default for the specified policy
ListEntitiesForPolicy - Lists all IAM users, groups, and roles that the specified managed policy is attached to
TagPolicy - Adds tags to a customer managed policy
UntagPolicy - Removes tags from a customer managed policy
ListPolicyTags - Lists the tags attached to a customer managed policy

The following methods are already there and support AWS Managed policies, need to ensure they work for customer managed policies also
AttachUserPolicy - Attaches a managed policy to a user AttachGroupPolicy - Attaches a managed policy to a group
AttachRolePolicy - Attaches a managed policy to a role
DetachUserPolicy - Detaches a managed policy from a user
DetachGroupPolicy - Detaches a managed policy from a group
DetachRolePolicy - Detaches a managed policy from a role
ListAttachedUserPolicies - Lists all managed policies attached to the specified user
ListAttachedGroupPolicies - Lists all managed policies attached to the specified group
ListAttachedRolePolicies - Lists all managed policies attached to the specified role



Class Diagram


UML : https://www.plantuml.com/plantuml/img/hLPRRzis57xdhpXmWLJEh68txC7DZ1REBdt86YGT-YZGrB7DbX8radBYEVtlEoHbMqubUHZsPFdmtBzpUsCTDwuk57i9SyMjXMl9LuONiDG6vhLrka03DxpaAypXLYifjd2_jGuBU91k3R_MfN1Ibp3dIbb6UgvnAKi4jqOGNYNQo4a4QQ10jzQvPIKlq5PS89ZLmtGgUR5ZV66TuS95ERZxxUFTB_SVtbSMTWpW6zXecKDbj41hCuD_rMXTqloEsHV6Zd94sNlVED_u7ZN9qi46ZLnkimfD8Qsb419T-KYuohOeLGxRCV6-2BnYZU9p9dXb4w5BwxoDFosQEL7WmjCZRuZbxTqVOD3gsWZSKpCkXAvBbr44ny_mEkHkKfFrdtxC70bvtoRW2JzyJmJlzsdGSuFSuL3OmqnDQ8jwkKIZf7KNi51PfQtB5ZeFsOYPawLqMOE3B5UTUMlGmaSKjSCUbcXGhBcv15yGoNZlvllgmxP2MA7B77qafv7-pijSeSbyUMJltjxillXdAfvQ16WwmtjNBhLFWxTWNJwT4Zzv7mnta4zGUlvOSIheds4qwdeBrh9SYxf0obOFMuvM61dIqiT25MgRvyrt9mn5hPmiU7NLOf-qPMVW-CgIP4pDiswpNErOHxlrrywBmRH2aKdwUHtGMsYjOCELpAdGY5xLhgciQrsxMqFZuV7Mw8tCQR9SvbLr0BsxAWSDUxDdv4zXaVaPzgWnLh2asSXLg7bYDHYY70vc4kF7Hw5gApSuWMFfImobJwXy-veTkac4ZZodDJwtg-FY457RnhwzZkt2SsqxG_Mufl7ztaTt2Lc8CkQdLoo9N9DMxl_7v7Z7dnNcjI5Mink4BKoAgetWrtf2aFzPMIkRGT7kJ9gwb90QQiBFnEJxq7ZPwRGwW15DHF6FHZhSZojV9W-Ua8GaRYSGPAdZjrOlFasWo9Mc0V2tBXjSABEBJnjwe361C3cct0Fc7Na9TwYuJxvTIrfY2tGFYEL-PRFMyhrwUlLgS4knmOrxzPIcPpOFwqKYskvDCcCHpyZN0fYrlbjA0fMTsPMNQVeqQwgE-Olc9jmiySA1vFJYeL7HiP0WJMVdLl8KHa99lo54K4mdK6IFxwCs16cZhp_BfEtvPJG64lhwoXcFXl5A_vtl7OgzlTQb50qDgV6MOfv9uVvK6AUpLklGlsP0UdK731kG1YgZjwH2UFrjws5f3AQprc0Zysn0p6jjTUwqFHD9KUnUAfOtMER-tlu7


