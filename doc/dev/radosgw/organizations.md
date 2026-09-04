# aws organizations

## concepts

limits https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html

organization
organizational unit
account
management account
member account

an existing account can call CreateOrganization to create an organization and become its management account
then create or invite member accounts and organize them into a hierarchy of organizational units
then create and apply policies (like service control policy) to organizational units or individual member accounts

## motivation

the AWS IAM api allows a user account to create distinct users/roles and manage their fine-grained access to account resources with iam policy. in ceph, cluster admins are responsible for creating new user accounts and establishing quota limits, but can then delegate the account root user's credentials to an end user for self-service management of the account and its resources

the AWS Organizations api enables this self-service management at a higher level, allowing one user account to grow into a hierarchical collection of related member accounts. these member accounts can be created/removed without the need for a ceph cluster admin, provided that they're all subject (in aggregate) to the same limits as the original user account. the management account can add service control policy to different parts of this hierarchy to further restrict which permissions its member accounts can grant themselves via iam policy

however, cluster admins may still take advantage of these features by keeping the organization's management account under their own control, and using organization apis to create member accounts on the behalf of end users. the admin can use service control policy to control which s3 features available to its members, enforce additional security policy, etc. the admin can also assume a member account's OrganizationAccountAccessRole to perform additional management/repair on their behalf

## api surface

Create/Delete/DescribeOrganization
Create/Delete/Describe/UpdateOrganizationalUnit
ListOrganizationalUnitsForParent
ListChildren/Parents/Roots
Close/Create/Describe/MoveAccount
ListAccounts, ListAccountsForParent
InviteAccountToOrganization (only thing in scope that requires handshake)
TagResource/UntagResource, ListTagsForResource
Attach/Create/Delete/Describe/Detach/UpdatePolicy
ListPolicies, ListPoliciesForTarget, ListTargetsForPolicy
DescribeEffectivePolicy
Disable/EnablePolicyType
ListHandshakesForAccount/Organization
Accept/Cancel/Decline/DescribeHandshake
policy types: SERVICE_CONTROL_POLICY

## not in scope

CreateGovCloudAccount: rgw has no concept of GovCloud regions
Deregister/RegisterDelegatedAdministrator, ListDelegatedAdministrators, ListDelegatedServicesForAccount: no delegated service admin
Delete/Describe/PutResourcePolicy: no "resource-based delegation policy" (see https://docs.aws.amazon.com/organizations/latest/userguide/orgs-policy-delegate.html)
EnableAWSServiceAccess, ListAWSServiceAccessForOrganization - no service access
EnableAllFeatures - all features enabled by default (no special handling for FeatureSet=CONSOLIDATED_BILLING)
LeaveOrganization, RemoveAccountFromOrganization - workaround for quota limits? one account can create several accounts
InviteOrganizationToTransferResponsibility, ListInboundResponsibilityTransfers, ListOutboundResponsibilityTransfers, TerminateResponsibilityTransfer, UpdateResponsibilityTransfer - the only type of responsibility is "BILLING" which rgw doesn't do
ListAccountsWithInvalidEffectivePolicy, ListEffectivePolicyValidationErrors - rgw doesn't really "validate" policy the way aws does
Describe/ListCreateAccountStatus - can probably just make CreateAccount synchronous
policy types: RESOURCE_CONTROL_POLICY | TAG_POLICY | BACKUP_POLICY | AISERVICES_OPT_OUT_POLICY | CHATBOT_POLICY | DECLARATIVE_POLICY_EC2 | SECURITYHUB_POLICY | INSPECTOR_POLICY | UPGRADE_ROLLOUT_POLICY | BEDROCK_POLICY | S3_POLICY | NETWORK_SECURITY_DIRECTOR_POLICY

## member account creation flow

a user in the organization's management account can use the CreateAccount action to create new member accounts, but that CreateAccount response does not include the new account root user's credentials

in aws, that new account's root user is created without a password or access keys. the new account owner uses their email address (which was specified in the CreateAccount request and verified to be unique) to initiate password recovery. this recovery allows them to log into the aws management console to add access keys to their root user

however, rgw
* doesn't have a management console for users (the ceph dashboard is for admins),
* doesn't have user passwords to recover,
* doesn't have any email integration

because rgw account root users don't have passwords, their credentials can't be disabled like in aws - the account owner would have no way to recover/reenable them. for member accounts, CreateAccount probably does need to create initial credentials for the account root user. but we need some other channel to make them available to the new account owner

email integration is probably the most straightforward way to accomplish this - by just sending the account root user credentials to the new account's email address, without any recovery process. email-based recovery for normal accounts/users might be an interesting feature to explore in the future. this initial email integration could be optional, but a cluster admin would otherwise be responsible for looking up these credentials and sharing them with the end user

CreateAccount requests are asynchronous, returning a CreateAccountStatus for pending account creation. after CreateAccount, this status can be polled with the ListCreateAccountStatus and DescribeCreateAccountStatus actions. if email integration is enabled but the delivery of these account credentials ultimately fails, that failure could be reported to the organization's management account through CreateAccountStatus

if email integration is not enabled, CreateAccountStatus could remain in that pending state until the cluster admin took some action to approve/deny it. however, this feature would ideally be entirely self-service for users and not require any admin intervention

# TODO

-[ ] data structures for organizations, units, etc
-[ ] sal interfaces
-[ ] rados object layout
-[ ] metadata sync
-[ ] rest apis
-[ ] service control policy interactions

