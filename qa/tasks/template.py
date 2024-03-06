"""
General template support for teuthology tasks.

Functions in this module allow tests to template strings. For example:
```
template.exec:
  host.x:
    - echo {{ctx.foo.bar}}
```

Functions like transform allow you to use this templating support
as a building block in your own .py files as well.

By default a template can access the variables `ctx` and `config` - these are
mapped to the first two arguments of this function and should match the ctx and
config passed to an individual task. Additional vars `cluster` and `VIP<N>`
(eg. VIP0, VIP1), `VIPPREFIXLEN`, `VIPSUBNET` are available for convenience
and/or backwards compatiblity with existing tests. Finally, keyword args may be
passed (via `ctx_vars`) to transform to add specific top-level
variables to extend the templating for specific use-cases.

Templates can access filters that transform on value into another. Currently,
the only filter, not part of the jinja2 default filters, available is
`role_to_remote`. Given a role name (like 'host.a') this returns the
teuthology remote object corresponding to the *first* matching role.
A template can then access the remote's properties as needed, for example to get
a host's IP address. Example:
```
template.exec:
  host.x:
    - pip install foobarbuzz
    - fbbuzz quuxify -q --extended {{role|role_to_remote|attr('ip_address')}}
```


"""

import functools
import logging

import jinja2
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def _convert_strs_in(obj, conv):
    """A function to walk the contents of a dict/list and recursively apply
    a conversion function (`conv`) to the strings within.
    """
    if isinstance(obj, str):
        return conv(obj)
    if isinstance(obj, dict):
        for k in obj:
            obj[k] = _convert_strs_in(obj[k], conv)
    if isinstance(obj, list):
        obj[:] = [_convert_strs_in(v, conv) for v in obj]
    return obj


def _apply_template(jinja_env, rctx, template):
    """Apply jinja2 templating to the template string `template` via the jinja
    environment `jinja_env`, passing a dictionary containing top-level context
    to render into the template.
    """
    if "{{" in template or "{%" in template:
        return jinja_env.from_string(template).render(**rctx)
    return template


@jinja2.pass_context
def _role_to_remote(rctx, role):
    """Return the first remote matching the given role."""
    ctx = rctx["ctx"]
    for remote, roles in ctx.cluster.remotes.items():
        if role in roles:
            return remote
    return None


def _vip_vars(rctx):
    """For backwards compat with the vip.subst_vip function."""
    # Make it possible to replace subst_vip in vip.py.
    ctx = rctx["ctx"]
    if "vnet" in getattr(ctx, "vip", {}):
        rctx["VIPPREFIXLEN"] = str(ctx.vip["vnet"].prefixlen)
        rctx["VIPSUBNET"] = str(ctx.vip["vnet"].network_address)
    if "vips" in getattr(ctx, "vip", {}):
        vips = ctx.vip["vips"]
        for idx, vip in enumerate(vips):
            rctx[f"VIP{idx}"] = str(vip)


def transform(ctx, config, target, **ctx_vars):
    """Apply jinja2 based templates to strings within the target object,
    returning a transformed target. Target objects may be a list or dict or
    str.

    Note that only string values in the list or dict objects are modified.
    Therefore one can read & parse yaml or json that contain templates in
    string values without the risk of changing the structure of the yaml/json.
    """
    jenv = getattr(ctx, "_jinja_env", None)
    if jenv is None:
        loader = jinja2.BaseLoader()
        jenv = jinja2.Environment(loader=loader)
        jenv.filters["role_to_remote"] = _role_to_remote
        setattr(ctx, "_jinja_env", jenv)
    rctx = dict(
        ctx=ctx, config=config, cluster_name=config.get("cluster", "")
    )
    _vip_vars(rctx)
    rctx.update(ctx_vars)
    conv = functools.partial(_apply_template, jenv, rctx)
    return _convert_strs_in(target, conv)


def expand_roles(ctx, config):
    """Given a context and a config dict containing a mapping of test roles
    to role actions (typically commands to exec). Expand the special role
    macros `all-roles` and `all-hosts` into role names that can be found
    in the teuthology config.
    """
    if "all-roles" in config and len(config) == 1:
        a = config["all-roles"]
        roles = teuthology.all_roles(ctx.cluster)
        config = dict(
            (id_, a) for id_ in roles if not id_.startswith("host.")
        )
    elif "all-hosts" in config and len(config) == 1:
        a = config["all-hosts"]
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if id_.startswith("host."))
    elif "all-roles" in config or "all-hosts" in config:
        raise ValueError(
            "all-roles/all-hosts may not be combined with any other roles"
        )
    return config


def exec(ctx, config):
    """
    This is similar to the standard 'exec' task, but does template substitutions.
    """
    assert isinstance(config, dict), "task exec got invalid config"
    testdir = teuthology.get_testdir(ctx)
    config = expand_roles(ctx, config)
    for role, ls in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info("Running commands on role %s host %s", role, remote.name)
        for c in ls:
            c.replace("$TESTDIR", testdir)
            remote.run(
                args=[
                    "sudo",
                    "TESTDIR={tdir}".format(tdir=testdir),
                    "bash",
                    "-ex",
                    "-c",
                    transform(ctx, config, c, role=role),
                ],
            )
