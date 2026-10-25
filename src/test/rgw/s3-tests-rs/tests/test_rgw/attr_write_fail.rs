//! Attribute writes made after an object exists, where the write fails.
//!
//! A copy's version id, a delete marker's attributes and a multipart
//! completion's version id are written once the file is in place.  The
//! write was unchecked at each, so a failure published an object
//! carrying no recorded id -- which a later demote files as the null
//! version -- or a delete marker without the attribute that makes it
//! one (n-plan D4).  Nothing in an ordinary setup makes these writes
//! fail, so the failure is injected (`inject-attr-write-fail`, keyed by
//! bucket so the tests running beside it are not touched).

use std::collections::HashMap;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    VersioningConfiguration,
};

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};

const HINT: &str = "inject-attr-write-fail";

fn fired(r: &HashMap<String, String>) -> u64 {
    r.get("fired").and_then(|s| s.parse().ok())
        .expect("the hint returned no count")
}

fn status<T, E>(r: &Result<T, aws_sdk_s3::error::SdkError<E>>) -> u16 {
    match r {
        Ok(_) => 200,
        Err(e) => e.raw_response().map_or(0, |r| r.status().as_u16()),
    }
}

/// Each of the three returns the failure, and leaves no marker behind.
///
/// For each operation:  armed for this bucket, it must fail and the
/// count must rise, so the failure is the injected one;  disarmed, the
/// same operation must succeed, which is the control.  The delete
/// additionally must leave no delete marker, since one without its
/// attributes would read as an empty object.  The hint is disarmed
/// before anything is asserted, so a failure cannot leave the bucket's
/// writes failing.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_attribute_writes_after_publish_return_their_failure() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features returned;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("{HINT} is nsfs-only;  skipped");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).send().await
        .expect("could not create the bucket");
    client.put_bucket_versioning().bucket(&bucket)
        .versioning_configuration(VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled).build())
        .send().await
        .expect("could not enable versioning");
    for key in ["src", "del"] {
        client.put_object().bucket(&bucket).key(key)
            .body(ByteStream::from_static(b"abc"))
            .send().await
            .expect("could not write an object");
    }
    let upload = |key: &'static str| {
        let client = client.clone();
        let bucket = bucket.clone();
        async move {
            let id = client.create_multipart_upload().bucket(&bucket).key(key)
                .send().await
                .expect("could not start an upload")
                .upload_id().expect("no upload id").to_string();
            let part = client.upload_part().bucket(&bucket).key(key)
                .upload_id(&id).part_number(1)
                .body(ByteStream::from_static(b"part"))
                .send().await
                .expect("could not upload a part");
            let done = CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1)
                    .e_tag(part.e_tag().unwrap_or_default()).build())
                .build();
            (id, done)
        }
    };
    let (mpu_id, mpu_done) = upload("mpu").await;

    /* The injected requests are sent once.  The SDK retries a 500, and
     * a completion retried after its upload finished is answered 200
     * by RGW, which would hide the first answer. */
    let once = aws_sdk_s3::Client::from_conf(client.config().to_builder()
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .build());

    /* the injected cases */
    let Some(armed) = driver_hint_results(
        HINT, &[("enable", "true"), ("bucket", &bucket)]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };
    let before = fired(&armed);
    let copy = once.copy_object().bucket(&bucket).key("copy")
        .copy_source(format!("{bucket}/src"))
        .send().await;
    let after_copy = fired(&driver_hint_results(
        HINT, &[("enable", "true"), ("bucket", &bucket)]).await
        .expect("the hint returned once and should return again"));
    let delete = once.delete_object().bucket(&bucket).key("del")
        .send().await;
    let after_delete = fired(&driver_hint_results(
        HINT, &[("enable", "true"), ("bucket", &bucket)]).await
        .expect("the hint returned once and should return again"));
    let complete = once.complete_multipart_upload().bucket(&bucket)
        .key("mpu").upload_id(&mpu_id).multipart_upload(mpu_done)
        .send().await;
    let after_complete = fired(&driver_hint_results(
        HINT, &[("enable", "false"), ("bucket", &bucket)]).await
        .expect("the hint returned once and should return again"));

    let markers = |key: &'static str| {
        let client = client.clone();
        let bucket = bucket.clone();
        async move {
            client.list_object_versions().bucket(&bucket).prefix(key)
                .send().await
                .expect("could not list versions")
                .delete_markers().iter()
                .filter(|m| m.key() == Some(key))
                .count()
        }
    };
    let markers_after_failed_delete = markers("del").await;

    /* the controls */
    let copy_ok = client.copy_object().bucket(&bucket).key("copy")
        .copy_source(format!("{bucket}/src"))
        .send().await;
    let copy_vid = copy_ok.as_ref().ok()
        .and_then(|r| r.version_id()).unwrap_or_default().to_string();
    let copy_read = client.get_object().bucket(&bucket).key("copy")
        .version_id(&copy_vid).send().await;
    let delete_ok = client.delete_object().bucket(&bucket).key("del")
        .send().await;
    let (mpu2_id, mpu2_done) = upload("mpu2").await;
    let complete_ok = client.complete_multipart_upload().bucket(&bucket)
        .key("mpu2").upload_id(&mpu2_id).multipart_upload(mpu2_done)
        .send().await;

    assert!(after_copy > before,
        "the copy's version-id write was not injected ({before} -> \
         {after_copy})");
    assert_eq!(status(&copy), 500,
        "a copy whose version id could not be recorded returned {}",
        status(&copy));
    assert!(after_delete > after_copy,
        "the delete marker's attribute write was not injected \
         ({after_copy} -> {after_delete})");
    assert_eq!(status(&delete), 500,
        "a delete whose marker could not be written returned {}",
        status(&delete));
    assert_eq!(markers_after_failed_delete, 0,
        "the failed delete left a delete marker without its attributes");
    assert!(after_complete > after_delete,
        "the completion's version-id write was not injected \
         ({after_delete} -> {after_complete})");
    assert_eq!(status(&complete), 500,
        "a completion whose version id could not be recorded returned {}",
        status(&complete));

    assert!(copy_ok.is_ok(), "the control copy failed: {copy_ok:?}");
    assert!(!copy_vid.is_empty(), "the control copy returned no version id");
    assert!(copy_read.is_ok(),
        "the control copy's version id names nothing: {copy_read:?}");
    assert!(delete_ok.as_ref().map(|r| r.delete_marker() == Some(true))
                .unwrap_or(false),
        "the control delete wrote no delete marker: {delete_ok:?}");
    assert_eq!(markers("del").await, 1,
        "the control delete's marker is not listed");
    assert!(complete_ok.is_ok(),
        "the control completion failed: {complete_ok:?}");
}
