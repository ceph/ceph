# -*- coding: utf-8 -*-
"""
Unit tests for ceph_secrets_types.py
"""
from __future__ import annotations

import pytest

from ceph_secrets_types import (
    CephSecretException,
    SecretScope,
    SecretRef,
    BadSecretURI,
    parse_secret_uri,
    parse_secret_path,
    SECRET_URI_SCHEME,
)

# ---------------------------------------------------------------------------
# SecretScope
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value, expected", [
    ("global",  SecretScope.GLOBAL),
    ("service", SecretScope.SERVICE),
    ("host",    SecretScope.HOST),
])
def test_secret_scope_from_str_valid(value, expected):
    assert SecretScope.from_str(value) == expected


@pytest.mark.parametrize("bad", [
    "GLOBAL",       # uppercase not accepted by from_str (only by _coerce_scope)
    "cluster",
    "",
    "  global  ",   # whitespace not stripped
])
def test_secret_scope_from_str_invalid(bad):
    with pytest.raises(CephSecretException, match="Invalid secret scope"):
        SecretScope.from_str(bad)


# ---------------------------------------------------------------------------
# SecretRef construction and invariants
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs, expected_ident", [
    (
        dict(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="reg-creds"),
        ("cephadm", "global", "", "reg-creds"),
    ),
    (
        dict(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert"),
        ("cephadm", "service", "ingress", "tls-cert"),
    ),
    (
        dict(namespace="cephadm", scope=SecretScope.HOST, target="host01", name="ssh-key"),
        ("cephadm", "host", "host01", "ssh-key"),
    ),
])
def test_secret_ref_ident(kwargs, expected_ident):
    ref = SecretRef(**kwargs)
    assert ref.ident() == expected_ident


@pytest.mark.parametrize("kwargs, error_fragment", [
    # empty namespace
    (dict(namespace="",        scope=SecretScope.GLOBAL,  target="",       name="n"),  "namespace"),
    # empty name
    (dict(namespace="ns",      scope=SecretScope.GLOBAL,  target="",       name=""),   "name"),
    # non-global with empty target
    (dict(namespace="ns",      scope=SecretScope.SERVICE, target="",       name="n"),  "target"),
    # slash in namespace
    (dict(namespace="ns/bad",  scope=SecretScope.GLOBAL,  target="",       name="n"),  "namespace"),
    # slash in name
    (dict(namespace="ns",      scope=SecretScope.GLOBAL,  target="",       name="a/b"),"name"),
    # slash in target
    (dict(namespace="ns",      scope=SecretScope.SERVICE, target="t/x",    name="n"),  "target"),
    # slash in key
    (dict(namespace="ns",      scope=SecretScope.GLOBAL,  target="",       name="n",
          key="k/v"),                                                                    "key"),
])
def test_secret_ref_construction_invalid(kwargs, error_fragment):
    # SecretRef.__post_init__ validates and wraps CephSecretException as ValueError.
    with pytest.raises(ValueError, match=error_fragment):
        SecretRef(**kwargs)


def test_secret_ref_is_frozen():
    ref = SecretRef(namespace="ns", scope=SecretScope.GLOBAL, target="", name="n")
    with pytest.raises((AttributeError, TypeError)):
        ref.name = "other"  # type: ignore[misc]


def test_secret_ref_is_hashable():
    ref1 = SecretRef(namespace="ns", scope=SecretScope.GLOBAL, target="", name="n")
    ref2 = SecretRef(namespace="ns", scope=SecretScope.GLOBAL, target="", name="n")
    assert ref1 == ref2
    assert hash(ref1) == hash(ref2)
    # can be used in a set
    s = {ref1, ref2}
    assert len(s) == 1


# ---------------------------------------------------------------------------
# SecretRef.to_uri() -- round-trip and format
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ref, expected_uri", [
    (
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="reg-creds"),
        "secret://cephadm/global/reg-creds",
    ),
    (
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert"),
        "secret://cephadm/service/ingress/tls-cert",
    ),
    (
        SecretRef(namespace="cephadm", scope=SecretScope.HOST, target="host01", name="ssh-key"),
        "secret://cephadm/host/host01/ssh-key",
    ),
    (
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="reg-creds", key="password"),
        "secret://cephadm/global/reg-creds?key=password",
    ),
    (
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert", key="cert"),
        "secret://cephadm/service/ingress/tls-cert?key=cert",
    ),
])
def test_secret_ref_to_uri(ref, expected_uri):
    assert ref.to_uri() == expected_uri


def test_secret_ref_to_uri_key_special_chars():
    """Keys with special characters must be percent-encoded in to_uri and survive round-trip."""
    ref = SecretRef(
        namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="creds",
        key="my key+val=x&y",
    )
    uri = ref.to_uri()
    assert "my+key" in uri or "my%20key" in uri  # urlencoded (space -> '+' or '%20' depending)
    # round-trip
    parsed_back = parse_secret_uri(uri)
    assert parsed_back.key == ref.key


# ---------------------------------------------------------------------------
# BadSecretURI
# ---------------------------------------------------------------------------

def test_bad_secret_uri_to_uri_returns_raw():
    bad = BadSecretURI(raw="secret://broken", error="some error", namespace="ns")
    assert bad.to_uri() == "secret://broken"


def test_bad_secret_uri_fields():
    bad = BadSecretURI(raw="x", error="msg", namespace="ns")
    assert bad.raw == "x"
    assert bad.error == "msg"
    assert bad.namespace == "ns"


# ---------------------------------------------------------------------------
# parse_secret_uri -- valid cases
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("uri, expected", [
    # global, no key
    (
        "secret://cephadm/global/reg-creds",
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="reg-creds"),
    ),
    # global with key
    (
        "secret://cephadm/global/reg-creds?key=password",
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="reg-creds", key="password"),
    ),
    # service scope
    (
        "secret://cephadm/service/ingress/tls-cert",
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert"),
    ),
    # service scope with key
    (
        "secret://cephadm/service/ingress/tls-cert?key=cert",
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert", key="cert"),
    ),
    # host scope
    (
        "secret://cephadm/host/host01/ssh-key",
        SecretRef(namespace="cephadm", scope=SecretScope.HOST, target="host01", name="ssh-key"),
    ),
    # different namespace
    (
        "secret://myns/global/mysecret",
        SecretRef(namespace="myns", scope=SecretScope.GLOBAL, target="", name="mysecret"),
    ),
])
def test_parse_secret_uri_valid(uri, expected):
    assert parse_secret_uri(uri) == expected


def test_parse_secret_uri_round_trip():
    """parse_secret_uri(ref.to_uri()) must equal the original ref for all scopes."""
    refs = [
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL,  target="",        name="reg-creds"),
        SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL,  target="",        name="reg-creds", key="password"),
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert"),
        SecretRef(namespace="cephadm", scope=SecretScope.SERVICE, target="ingress", name="tls-cert",  key="cert"),
        SecretRef(namespace="cephadm", scope=SecretScope.HOST,    target="host01",  name="ssh-key"),
    ]
    for ref in refs:
        assert parse_secret_uri(ref.to_uri()) == ref


# ---------------------------------------------------------------------------
# parse_secret_uri -- invalid cases (structural)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("uri, error_fragment", [
    # wrong type
    (42,                                          "must be a string"),
    (None,                                        "must be a string"),
    # wrong scheme
    ("http://ns/global/name",                     "Not a secret uri"),
    ("https://ns/global/name",                    "Not a secret uri"),
    ("",                                          "Not a secret uri"),
    # fragment form rejected
    ("secret://cephadm/global/name#foo",          "fragment form"),
    ("secret://cephadm/global/name?key=x#bar",    "fragment form"),
    # missing namespace
    ("secret:///global/name",                     "missing namespace"),
    # short forms rejected with targeted message
    ("secret://global/name",                      "short forms are not supported"),
    ("secret://service/tgt/name",                 "short forms are not supported"),
    ("secret://host/tgt/name",                    "short forms are not supported"),
    # empty path
    ("secret://cephadm/",                         "missing scope"),
    # double slash in path
    ("secret://cephadm/global//name",             "empty path segment"),
    # trailing slash
    ("secret://cephadm/global/name/",             "empty path segment"),
    ("secret://cephadm/service/tgt/name/",        "empty path segment"),
    # unknown scope
    ("secret://cephadm/cluster/tgt/name",         "Invalid secret scope"),
])
def test_parse_secret_uri_invalid_structure(uri, error_fragment):
    with pytest.raises(CephSecretException, match=error_fragment):
        parse_secret_uri(uri)


# ---------------------------------------------------------------------------
# parse_secret_uri -- segment count enforcement
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("uri, error_fragment", [
    # global: too few (scope only)
    ("secret://cephadm/global",                   "Expected"),
    # global: too many
    ("secret://cephadm/global/name/extra",        "global scope requires exactly one"),
    ("secret://cephadm/global/a/b/c",             "global scope requires exactly one"),
    # non-global: too few (scope + target only)
    ("secret://cephadm/service/ingress",          "requires exactly"),
    # non-global: too many
    ("secret://cephadm/service/a/b/c",            "requires exactly"),
    ("secret://cephadm/host/a/b/c/d",             "requires exactly"),
])
def test_parse_secret_uri_segment_count(uri, error_fragment):
    with pytest.raises(CephSecretException, match=error_fragment):
        parse_secret_uri(uri)


# ---------------------------------------------------------------------------
# parse_secret_uri -- key query parameter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("uri, error_fragment", [
    # empty key
    ("secret://cephadm/global/name?key=",         "must not be empty"),
    # whitespace-only key
    ("secret://cephadm/global/name?key=   ",      "must not be empty"),
    # duplicate key parameter
    ("secret://cephadm/global/name?key=a&key=b",  "multiple key parameters"),
])
def test_parse_secret_uri_bad_key(uri, error_fragment):
    with pytest.raises(CephSecretException, match=error_fragment):
        parse_secret_uri(uri)


def test_parse_secret_uri_no_key_returns_none():
    ref = parse_secret_uri("secret://cephadm/global/reg-creds")
    assert ref.key is None


def test_parse_secret_uri_key_with_special_chars():
    """Keys with spaces and symbols survive urlencoding and are recovered correctly."""
    original_key = "my key+val=x&y"
    ref = SecretRef(namespace="cephadm", scope=SecretScope.GLOBAL, target="", name="n", key=original_key)
    parsed = parse_secret_uri(ref.to_uri())
    assert parsed.key == original_key


# ---------------------------------------------------------------------------
# parse_secret_uri -- percent-encoding
# ---------------------------------------------------------------------------

def test_parse_secret_uri_percent_encoded_slash_in_name_rejected():
    """%2F in a name decodes to '/' which must be rejected by component validation."""
    with pytest.raises(CephSecretException, match="must not contain"):
        parse_secret_uri("secret://cephadm/global/my%2Fsecret")


def test_parse_secret_uri_double_percent_encoding_single_decode():
    """%2525 must decode to %25secret, not %secret (single-decode invariant)."""
    ref = parse_secret_uri("secret://cephadm/global/%2525secret")
    assert ref.name == "%25secret"


def test_parse_secret_uri_error_type_is_ceph_secret_exception():
    """All parse errors must be CephSecretException, not bare ValueError or TypeError."""
    bad_uris = [
        "http://ns/global/name",
        "secret:///global/name",
        "secret://global/name",
        42,
    ]
    for uri in bad_uris:
        with pytest.raises(CephSecretException):
            parse_secret_uri(uri)


# ---------------------------------------------------------------------------
# parse_secret_path -- valid cases
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path, expected", [
    # 3-part global
    (
        "cephadm/global/reg-creds",
        ("cephadm", SecretScope.GLOBAL, "", "reg-creds"),
    ),
    # 3-part global, uppercase scope
    (
        "cephadm/GLOBAL/reg-creds",
        ("cephadm", SecretScope.GLOBAL, "", "reg-creds"),
    ),
    # 4-part service
    (
        "cephadm/service/ingress/tls-cert",
        ("cephadm", SecretScope.SERVICE, "ingress", "tls-cert"),
    ),
    # 4-part host
    (
        "cephadm/host/host01/ssh-key",
        ("cephadm", SecretScope.HOST, "host01", "ssh-key"),
    ),
    # single leading slash tolerated
    (
        "/cephadm/global/reg-creds",
        ("cephadm", SecretScope.GLOBAL, "", "reg-creds"),
    ),
    (
        "/cephadm/service/ingress/tls-cert",
        ("cephadm", SecretScope.SERVICE, "ingress", "tls-cert"),
    ),
    # outer whitespace stripped from input
    (
        "  cephadm/global/reg-creds  ",
        ("cephadm", SecretScope.GLOBAL, "", "reg-creds"),
    ),
])
def test_parse_secret_path_valid(path, expected):
    assert parse_secret_path(path) == expected


# ---------------------------------------------------------------------------
# parse_secret_path -- invalid cases
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path, error_fragment", [
    # empty / blank
    ("",                                  "empty"),
    ("   ",                               "empty"),
    # multiple leading slashes
    ("//cephadm/global/name",             "multiple leading slashes"),
    ("///cephadm/global/name",            "multiple leading slashes"),
    # internal double slash
    ("cephadm//global/name",              "empty segment"),
    ("cephadm/global//name",              "empty segment"),
    # trailing slash
    ("cephadm/global/name/",              "empty segment"),
    ("cephadm/service/tgt/name/",         "empty segment"),
    # whitespace inside segments (not silently stripped)
    ("cephadm / global / name",           "whitespace"),
    # wrong segment count
    ("cephadm/global",                    "Use '<namespace>"),
    ("cephadm",                           "Use '<namespace>"),
    ("cephadm/service/a/b/c",             "Use '<namespace>"),
    # 4-part global (must use 3-part form)
    ("cephadm/global/target/name",        "global scope cannot have a target"),
    # 3-part non-global (must use 4-part form)
    ("cephadm/service/name",              "3-part form is only valid for global"),
    ("cephadm/host/name",                 "3-part form is only valid for global"),
    # unknown scope
    ("cephadm/cluster/tgt/name",          "Unknown scope"),
])
def test_parse_secret_path_invalid(path, error_fragment):
    with pytest.raises(CephSecretException, match=error_fragment):
        parse_secret_path(path)


def test_parse_secret_path_non_global_scope_error_names_correct_form():
    """Error for 3-part non-global must name the correct 4-part form."""
    with pytest.raises(CephSecretException, match=r"<namespace>/service/<target>"):
        parse_secret_path("cephadm/service/mysecret")


def test_parse_secret_path_global_with_target_error_names_3part_form():
    """Error for 4-part global must name the 3-part form."""
    with pytest.raises(CephSecretException, match="3-part form"):
        parse_secret_path("cephadm/global/target/name")


def test_parse_secret_uri_double_slash_after_authority_is_urlparse_quirk():
    """
    Document current behaviour: urlparse interprets 'secret://ns//path' as netloc='ns', path='//path'.
    lstrip('/') then reduces '//path' to 'path', so the double-slash is swallowed silently and the URI
    parses as if it were 'secret://ns/path'.

    This is undesirable but kept as a documentation test until parse_secret_uri is hardened
    to detect this quirk before urlparse (or by inspecting the raw URI string).
    """
    result = parse_secret_uri('secret://cephadm//global/name')
    assert result == SecretRef(
        namespace='cephadm', scope=SecretScope.GLOBAL, target='', name='name'
    )


def test_parse_secret_path_trailing_tab_stripped_by_outer_strip():
    """
    path.strip() at the top of parse_secret_path strips ALL surrounding whitespace including tabs,
    so a trailing tab on the whole input is silently consumed before segment-level checks run.
    Only whitespace INSIDE a segment (after the split) is detected and rejected.
    """
    # trailing tab on the input -> stripped, parses fine
    result = parse_secret_path('cephadm/global/name\t')
    assert result == ('cephadm', SecretScope.GLOBAL, '', 'name')

    # tab as leading whitespace in a segment -> caught by s != s.strip()
    with pytest.raises(CephSecretException, match='whitespace'):
        parse_secret_path('cephadm/global/\tname')


# ---------------------------------------------------------------------------
# SecretURI protocol -- both SecretRef and BadSecretURI satisfy it
# ---------------------------------------------------------------------------

def test_secret_ref_satisfies_secret_uri_protocol():
    ref = SecretRef(namespace="ns", scope=SecretScope.GLOBAL, target="", name="n")
    assert callable(ref.to_uri)
    assert isinstance(ref.to_uri(), str)
    assert hash(ref) is not None


def test_bad_secret_uri_satisfies_secret_uri_protocol():
    bad = BadSecretURI(raw="secret://broken", error="oops", namespace="ns")
    assert callable(bad.to_uri)
    assert bad.to_uri() == "secret://broken"
    assert hash(bad) is not None


def test_both_types_usable_in_same_set():
    """scan_refs returns Set[SecretURI] which can mix SecretRef and BadSecretURI."""
    ref = SecretRef(namespace="ns", scope=SecretScope.GLOBAL, target="", name="n")
    bad = BadSecretURI(raw="secret://broken", error="oops", namespace="ns")
    mixed: set = {ref, bad}
    assert len(mixed) == 2
    uris = {item.to_uri() for item in mixed}
    assert f"{SECRET_URI_SCHEME}ns/global/n" in uris
    assert "secret://broken" in uris
