# account email recovery

rgw accounts don't have passwords for conventional login, but they do require a unique email address. use this email address for secure account recovery features:
* remove root user credentials when not in use
* add credentials back when needed
* recover lost root user credentials

## security

* replay attacks: recovery links are single-use expire after x minutes
* transport security: TLS for http, STARTTLS for email?
* same http response whether email was valid or not
* recovery attempts are rate limited (per-requester?)

how to secure the url? does it include the account id, or is that encrypted in a token?

this stuff will probably require us to store some server state in the account, but we don't want to replicate that state via metadata sync.. so use a separate rados object for recovery?

## questions

1. what if account has multiple root users? recovery not supported?
2. what configuration is necessary to send email?
3. where to put this api? under /admin/ so it doesn't conflict with s3 namespace? or require separate dns name?
4. how to discover the dns name for recovery links sent to email? https://what.goes.here.com/account/recovery/add

a config option like rgw_dns_recovery_name could resolve 3 and 4

## reify account root user

rgw differs from aws in that each account may have multiple account root users. `radosgw-admin user modify --account-root` just sets `RGWUserInfo::type` to `TYPE_ROOT`, and nothing prevents this from applying to several users in the account

even if the account has several root users, this recovery logic would rely on the account having one designated root user for recovery purposes. that root user's id could be stored in `RGWAccountInfo::root_uid` and set by the admin with `radosgw-admin account modify --root-user=<uid>` to opt into the recovery feature

we might also want an option like `radosgw-admin account create --root-user` that would automatically create and attach this root user during account creation

## rest apis

### /account/welcome

```
Welcome to your user account

An account root user has been created for you with full permissions to the resources of your account. Before you can use this root user, you must enable API credentials for it. To do so, enter the email address associated with your account and click Add Credentials. A secure link will be sent to your email address that will generate and show your new credentials.

It is strongly recommended that you use this root user to create additional IAM users with CreateUser and CreateAccessKey, then add policy to grant the minimal permissions required. While the account root user is not in use, its credentials should be removed. Visit /account/root to remove root user credentials or add them back.

Account email address: _________

[Add Credentials]
```

### /account/root

```
User account recovery

The account root user has full permissions to the resources of your account. It can use the IAM api to create/remove other account users and manage their credentials. It is strongly recommended that you create additional IAM users with CreateUser and CreateAccessKey, then add policy to grant the minimal permissions required. While the account root user is not in use, its credentials should be removed. This form can also be used to generate new account root user credentials or replace existing credentials.

Account email address: _________

[Add Credentials]
[Remove Credentials]
```

### /account/root/add

```
A secure link has been sent the associated email address. Follow that link to generate and display the new credentials.
```

if account exists with that email, send email with a secure link to /account/recovery/add

### /account/root/remove

```
A secure link has been sent the associated email address. Follow that link to remove your credentials.
```

if account exists with that email, send email with a secure link to /account/recovery/remove

### /account/recovery/add

send email alert that credentials were added

```
Added new credentials to the account root user:
(or)
Replaced existing credentials for account root user:

Access Key ID:      ___________
Secret Access Key:  ___________
```

### /account/recovery/remove

send email alert that credentials were added

```
Removed credentials from the account root user. Visit /account/root to add them back.
```

## customization

deployments may want to customize the contents of the recovery pages or the subject/body of emails. we could provide templates for these as config options

## eventual dashboard integration

while the ceph dashboard is currently for admins, it may eventually support user-facing logins. it would be natural for the dashboard to provide its own pages instead of rgw's /account/welcome and /account/root, as long as they submit the same forms to /account/root/add and /account/root/remove and know the configured rgw_dns_recovery_name
