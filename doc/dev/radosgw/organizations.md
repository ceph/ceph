# aws organizations

limits https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html

organization
organizational unit
account
management account
member account

an existing account can call CreateOrganization to create an organization and become its management account
then create or invite member accounts and organize them into a hierarchy of organizational units
then create and apply policies (like service control policy) to organizational units or individual member accounts

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

# TODO

-[ ] data structures for organizations, units, etc
-[ ] sal interfaces
-[ ] rados object layout
-[ ] metadata sync
-[ ] rest apis
-[ ] service control policy interactions

