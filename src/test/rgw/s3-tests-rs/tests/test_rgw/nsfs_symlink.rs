//! A symbolic link in a bucket is not an object (DECISIONS.md
//! 2026-10-23;  ceph-cp `784be57987d`):  it is not listed, a read of its
//! name is NoSuchKey, a PUT of its name replaces it, a DELETE of its
//! name unlinks it and never its target, and it does not keep its bucket
//! from being deleted.  Adoption counts every link it meets
//! (`50b14452f88`).
//!
//! A filesystem client makes links and S3 cannot, so the gateway makes
//! them through the `symlink` driver hint, and the tests need no access
//! to the gateway's filesystem.  Under impersonation a link is made as
//! the main user, as that identity's filesystem client would make it,
//! so its own requests may remove it under every access model.

use std::collections::HashMap;

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::admin::{driver_hint, driver_hint_results, nsfs_set_profile, q};
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

async fn impersonating() -> bool {
    match features().await {
        Some(f) => f.get_bool("impersonate").unwrap_or(false),
        None => false,
    }
}

/// A link at `key` in `bucket`, pointing at `target`.  `None` where the
/// hint is not reachable, so the caller skips.
async fn link(bucket: &str, key: &str, target: &str)
    -> Option<HashMap<String, String>> {
    let target = q(target);
    let me = get_config().main_user_id.clone();
    let mut params = vec![("op", "create"), ("bucket", bucket), ("key", key),
                          ("target", target.as_str())];
    if impersonating().await {
        params.push(("as", me.as_str()));
    }
    let r = driver_hint_results("symlink", &params).await?;
    assert_eq!(r.get("kind").map(String::as_str), Some("link"),
        "the hint did not make a link at {key}");
    Some(r)
}

/// What is at `key` on the filesystem:  link, file, directory, other or
/// none.
async fn kind(bucket: &str, key: &str) -> String {
    driver_hint_results("symlink", &[("op", "stat"), ("bucket", bucket),
                                     ("key", key)])
        .await
        .and_then(|r| r.get("kind").cloned())
        .expect("the symlink hint answered the create but not the stat")
}

async fn put(bucket: &str, key: &str, body: &str) {
    get_client().put_object().bucket(bucket).key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send().await
        .unwrap_or_else(|e| panic!("PUT {key}: {e:?}"));
}

async fn body(bucket: &str, key: &str) -> String {
    let out = get_client().get_object().bucket(bucket).key(key)
        .send().await
        .unwrap_or_else(|e| panic!("GET {key}: {e:?}"));
    String::from_utf8(out.body.collect().await.unwrap().into_bytes().to_vec())
        .unwrap()
}

/// The links `plant` makes.
const LINKS: [&str; 6] = ["l-file", "l-abs", "l-dir", "l-out", "l-dangling",
                          "l-loop"];

/// One link of each kind beside the objects `o00` and `sub/inner`:  to
/// a file in the bucket, relative and absolute, to a directory in it,
/// out of it, to nothing, and to itself.  `None` where the hint is not
/// reachable.
async fn plant(bucket: &str) -> Option<()> {
    put(bucket, "o00", "o00").await;
    put(bucket, "sub/inner", "inner").await;
    let first = link(bucket, "l-file", "o00").await?;
    let root = first.get("bucket_path")
        .expect("the hint reports no bucket path")
        .clone();
    link(bucket, "l-abs", &format!("{root}/o00")).await?;
    link(bucket, "l-dir", "sub").await?;
    link(bucket, "l-out", "..").await?;
    link(bucket, "l-dangling", "nowhere").await?;
    link(bucket, "l-loop", "l-loop").await?;
    Some(())
}

/// One link in a directory ended its listing:  the walk stopped on it,
/// and the listing cache kept what it had.  Twenty objects, so readdir
/// order cannot put the links last, and a cold fill, so the walk runs.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_symlink_is_not_listed_and_does_not_end_the_listing() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    if plant(&bucket).await.is_none() {
        eprintln!("symlink hint unavailable;  nothing to check");
        return;
    }
    let mut want = vec!["o00".to_string(), "sub/inner".to_string()];
    for i in 1..20 {
        let k = format!("o{i:02}");
        put(&bucket, &k, &k).await;
        want.push(k);
    }
    let r = driver_hint("invalidate-cache", &[("bucket", &bucket)]).await;
    assert_eq!(r.status, 200, "invalidating the listing cache: {}", r.body);

    let listed: Vec<String> = client.list_objects_v2().bucket(&bucket)
        .send().await.unwrap()
        .contents().iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    for k in &want {
        assert!(listed.contains(k), "{k} is missing from the listing");
    }
    for k in &listed {
        assert!(!k.starts_with("l-"), "{k}, a link, was listed");
    }
}

/// A key that names a link is NoSuchKey, whatever the link points at.
/// The control is `o00`, which two of the links name.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_symlink_reads_as_no_such_key() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    if plant(&bucket).await.is_none() {
        eprintln!("symlink hint unavailable;  nothing to check");
        return;
    }
    for k in LINKS {
        let r = client.get_object().bucket(&bucket).key(k).send().await;
        s3_tests_rs::assert_s3_err!(r, 404, "NoSuchKey");
    }
    assert_eq!(body(&bucket, "o00").await, "o00");
}

/// A DELETE of a link's name unlinks the link, never its target:  the
/// one S3 operation that clears a name no listing shows.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_deleting_a_symlinks_name_unlinks_only_the_link() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    if plant(&bucket).await.is_none() {
        eprintln!("symlink hint unavailable;  nothing to check");
        return;
    }
    for k in LINKS {
        client.delete_object().bucket(&bucket).key(k).send().await
            .unwrap_or_else(|e| panic!("DELETE {k}: {e:?}"));
        assert_eq!(kind(&bucket, k).await, "none", "{k} is still there");
    }
    assert_eq!(body(&bucket, "o00").await, "o00");
    assert_eq!(body(&bucket, "sub/inner").await, "inner");
    assert_eq!(kind(&bucket, "sub").await, "directory");
}

/// A PUT of a link's name replaces the link with the object, and the
/// link's target is untouched -- under If-None-Match:  * as well, since a
/// link is not an object for the condition to find.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_put_of_a_symlinks_name_replaces_it() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    if plant(&bucket).await.is_none() {
        eprintln!("symlink hint unavailable;  nothing to check");
        return;
    }
    put(&bucket, "l-out", "replaced").await;
    assert_eq!(kind(&bucket, "l-out").await, "file");
    assert_eq!(body(&bucket, "l-out").await, "replaced");

    client.put_object().bucket(&bucket).key("l-file").if_none_match("*")
        .body(ByteStream::from_static(b"excl"))
        .send().await
        .expect("If-None-Match:  * found a link to refuse on");
    assert_eq!(kind(&bucket, "l-file").await, "file");
    assert_eq!(body(&bucket, "l-file").await, "excl");
    assert_eq!(body(&bucket, "o00").await, "o00");
}

/// A bucket is empty when it lists nothing, so links do not keep it, and
/// its delete removes them.  The control is the same bucket while it
/// still holds an object.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_bucket_holding_only_symlinks_is_deleted() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    put(&bucket, "o00", "o00").await;
    if link(&bucket, "l-out", "..").await.is_none() {
        eprintln!("symlink hint unavailable;  nothing to check");
        return;
    }
    link(&bucket, "l-dangling", "nowhere").await.unwrap();
    link(&bucket, "l-loop", "l-loop").await.unwrap();

    let r = client.delete_bucket().bucket(&bucket).send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "BucketNotEmpty");

    client.delete_object().bucket(&bucket).key("o00").send().await.unwrap();
    client.delete_bucket().bucket(&bucket).send().await
        .expect("a bucket holding only links was not deleted");
}

/// Adoption counts every link by what it resolves to, as NooBaa resolved
/// it, and converts none.  The bucket is unmarked through the test hint,
/// which takes only an empty bucket, and a bucket not taken over takes
/// no client writes, so no link here can name a file:  the file class is
/// the unit binary's (TheConversionCountsEveryLinkByWhatItResolvesTo).
/// Converting it adopts it, so cleanup can delete it.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_adoption_counts_every_symlink() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let Some(r) = driver_hint_results("unmarked-bucket", &[("bucket", &bucket)])
        .await else {
        eprintln!("unmarked-bucket hint unavailable;  nothing to check");
        return;
    };
    assert_eq!(r.get("profile").map(String::as_str), Some("base"));

    let Some(first) = link(&bucket, "l-dir", ".").await else {
        eprintln!("symlink hint unavailable;  adopting and stopping");
        nsfs_set_profile(&bucket, "shared").await;
        return;
    };
    let root = first.get("bucket_path").unwrap().clone();
    link(&bucket, "l-abs", &root).await.unwrap();
    link(&bucket, "l-out", "..").await.unwrap();
    link(&bucket, "l-dangling", "nowhere").await.unwrap();
    link(&bucket, "l-loop", "l-loop").await.unwrap();

    let up = nsfs_set_profile(&bucket, "shared").await
        .expect("the profile endpoint did not answer");
    let count = |k: &str| up.get(&format!("links.{k}")).cloned();
    assert_eq!(count("file").as_deref(), Some("0"));
    assert_eq!(count("directory").as_deref(), Some("2"),
        "l-dir and the absolute l-abs, both the bucket itself");
    assert_eq!(count("outside").as_deref(), Some("1"));
    assert_eq!(count("dangling").as_deref(), Some("1"));
    assert_eq!(count("loop").as_deref(), Some("1"));
    assert_eq!(up.get("converted.objects").map(String::as_str), Some("0"),
        "a link was converted as an object");
    for k in ["l-dir", "l-abs", "l-out", "l-dangling", "l-loop"] {
        assert_eq!(kind(&bucket, k).await, "link", "{k} was touched");
    }
}
