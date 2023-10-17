# iam accounts

add a user account primitive for use with IAM REST APIs. this enables end-users to use tools like `aws iam create-user` for self-service management of the users, roles, policies, and access keys in their account without requiring global admin privileges for radosgw-admin commands or radosgw /admin REST APIs

add new IAM APIs for User and AccessKey operations. some Role/RolePolicy/UserPolicy operations are already supported, but require special admin capabilities. once a User or Role are added to an account, that account's root user will be granted full access to them

existing users and roles must be added to an account manually, either by a radosgw-admin command or /admin API, before they can be managed by an account root user


## namespacing

rgw supports namespace isolation of users and buckets using the 'tenant' name, such that two tenants A and B can both have buckets named 'example-bucket', or users named 'example-user', that are completely indepedent of one another

there exists no 'tenant' metadata that we can attach policy to, index users or buckets on, or accumulate usage stats for reporting or quota enforcement. features like this will instead be attached to the proposed account metadata

to preserve this namespacing functionality, each account can optionally be associated with a tenant name. all users and roles in that account must have that same tenant name


## account metadata

accounts are stored as metadata objects and replicated in multisite by metadata sync

account metadata is created/modified/deleted through a set of radosgw-admin commands ('account create/get/modify/rm') and /admin/account APIs that require admin permissions

the account metadata maintains an index of its users and roles to support API operations like ListUsers and ListRoles

the bucket, user, and role metadata have a new 'account_id' field which refers to their account, if any


### account identifiers

each account is identified both by an id and a name

the account id is the API-visible string used in iam policy documents. for example, a Principal could refer to the account root user with `{accountid}:root`, another user with `{accountid}:user/{userid}`, or a role with `{accountid}:role/{roleid}`

the account id must be of the special form "RGW#################" (where # is a numeric character). this allows us to disambiguate account ids from tenant names which can already appear here in iam policy. on account creation, a random account id is generated unless one is given

the account name is used only as a convenience for these admin commands/APIs. unlike the account ids, account names are namespaced according to the optional tenant name


### account stats and quota

account/user/bucket quotas are disabled by default. they can be enabled/configured by the cluster admin, not the account root user

quota enforcement in rgw depends on usage statistics. each bucket index shard is the source of truth for the entries it contains

over time, the rgw quota cache accumulates the updated bucket shard stats into bucket-wide stats for the enforcement of bucket quotas. eventually, these bucket stats also get accumulated into the user's stats for enforcement of user quotas

for user stats, the stats of its individual buckets are stored in omap entries on the `{userid}.buckets` object. whenever a bucket's stats are recalculated, they're written to that bucket's omap entry and cls_user updates the accumulated user stats accordingly

for account stats, we could add another level of indirection to accumulate user stats in an `{accountid}.users` object. however, i believe it's probably simpler for the quota cache to write its bucket stats to both the `{userid}.buckets` and the `{accountid}.buckets` objects at the same time

this requires the account metadata to also manage the index of all buckets under that account, and link/unlink them as buckets are created/deleted or added/removed from the account. it also has the same scaling limitations as the `{userid}.buckets` object with respect to the number of omap entries on a single rados object

we should be able to use the same cls_user calls for `{accountid}.buckets` that we already use for `{userid}.buckets`. so if we solve this scaling problem for users, we could apply the same solution to accounts


## account limits

the goal of quota enforcement is to prevent a single account or user from consuming too much storage capacity. but quotas are measured only in total objects and size of object data

for users, rgw also imposes a limit on bucket creation to 1000 per user by default. this prevents unbounded creation of bucket metadata, and also works around the scaling limitations of `{userid}.buckets` mentioned above

at the account level, there should be similar limits on the number of users and roles created

open questions:
* do we need to limit the number of buckets per account, or just rely on (users-per-account * buckets-per-user)?
* what about access keys per user?


## iam groups and group policy?

we don't have any support for groups yet. is there interest in groups and group policy? this seems relatively easy to add as future work
