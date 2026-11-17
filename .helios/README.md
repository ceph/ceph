# Helios Voter List

Format documented in the [Helios User Manual](https://github.com/benadida/helios-server/blob/master/docs/user-manual.md#4-add-voters).

## Voter Credentials

You may have per-election credentials sent to your email:

```csv
password,<userid>,<email>,<name>
```

where `<userid>` is an arbitrary unique identifier, and `<email>` is used by Helios to deliver credentials.

Or, you may have a GitHub login:

```csv
github,<username>,<email>,<name>
```

Everything after `<username>` is ignored by Helios and used for documenting
voter metadata (e.g. email address, full name) within this repository.

> [!WARNING]
> The administrator of the election will see whatever public email you have
> associated with your GitHub `<username>`. That email (**not** the one in
> the CSV) will receive election correspondence. Ensure your GitHub account
> is configured with the email you expect to receive ballots at. Your GitHub
> email is **not** visible on the public election page.

## Supported Authentication Types

Helios also reports supporting the following voter types:

`password`, `linkedin`, `cas`, `facebook`, `google`, `yahoo`, `github`, `ldap`, `gitlab`

> [!CAUTION]
> You may test yourself if you prefer one of those other authentication types.
> However, please verify they work correctly with Helios beforehand, as they
> may not have been fully verified for this repository.
