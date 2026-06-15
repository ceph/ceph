use s3_tests_rs::client::{
    get_iam_root_s3client,
    get_sns_root_client, get_sns_alt_root_client,
    get_iam_alt_root_s3client,
};
use s3_tests_rs::fixtures::get_new_bucket_name;

async fn nuke_topics(client: &aws_sdk_sns::Client, prefix: &str) {
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_topics();
        if let Some(ref token) = next_token {
            req = req.next_token(token);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => break,
        };
        for topic in resp.topics() {
            if let Some(arn) = topic.topic_arn() {
                if arn.contains(prefix) {
                    let _ = client.delete_topic().topic_arn(arn).send().await;
                }
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }
}

fn get_topic_prefix() -> String {
    let cfg = s3_tests_rs::config::get_config();
    cfg.bucket_prefix.replace("{random}", "").trim_matches('-').to_string()
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SNS not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SNS not implemented")]
#[tokio::test]
async fn test_account_topic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let name = get_new_bucket_name();

    let response = sns.create_topic().name(&name).send().await.unwrap();
    let arn = response.topic_arn().unwrap();
    assert!(arn.starts_with("arn:aws:sns:"), "ARN should start with arn:aws:sns:, got: {arn}");
    assert!(arn.ends_with(&format!(":{name}")), "ARN should end with :{name}, got: {arn}");

    let response = sns.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(arns.contains(&arn), "created topic not in list");

    sns.set_topic_attributes()
        .topic_arn(arn)
        .attribute_name("Policy")
        .attribute_value("")
        .send()
        .await
        .unwrap();

    let response = sns.get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(response.attributes().is_some());

    sns.delete_topic().topic_arn(arn).send().await.unwrap();

    let response = sns.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(!arns.contains(&arn), "deleted topic still in list");

    let result = sns.get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await;
    assert!(result.is_err(), "expected NotFound for deleted topic");

    // delete of already-deleted topic should succeed
    sns.delete_topic().topic_arn(arn).send().await.unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SNS not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SNS not implemented")]
#[tokio::test]
async fn test_cross_account_topic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let sns_alt = get_sns_alt_root_client();
    let name = get_new_bucket_name();

    let arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    // alt user should not be authorized
    let result = sns_alt.get_topic_attributes().topic_arn(&arn).send().await;
    assert!(result.is_err(), "expected AuthorizationError for alt user get_topic_attributes");

    let result = sns_alt.set_topic_attributes()
        .topic_arn(&arn)
        .attribute_name("Policy")
        .attribute_value("")
        .send()
        .await;
    assert!(result.is_err(), "expected AuthorizationError for alt user set_topic_attributes");

    let result = sns_alt.delete_topic().topic_arn(&arn).send().await;
    assert!(result.is_err(), "expected AuthorizationError for alt user delete_topic");

    sns.delete_topic().topic_arn(&arn).send().await.unwrap();

    let response = sns_alt.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(!arns.contains(&arn.as_str()), "deleted topic visible to alt user");

    nuke_topics(&sns, &get_topic_prefix()).await;
    nuke_topics(&sns_alt, &get_topic_prefix()).await;
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SNS not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SNS not implemented")]
#[tokio::test]
async fn test_account_topic_publish() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let s3 = get_iam_root_s3client();
    let name = get_new_bucket_name();

    let topic_arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    let bucket = get_new_bucket_name();
    s3.create_bucket().bucket(&bucket).send().await.unwrap();

    s3.put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(
            aws_sdk_s3::types::NotificationConfiguration::builder()
                .topic_configurations(
                    aws_sdk_s3::types::TopicConfiguration::builder()
                        .id("id")
                        .topic_arn(&topic_arn)
                        .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SNS not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SNS not implemented")]
#[tokio::test]
async fn test_cross_account_topic_publish() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let s3_alt = get_iam_alt_root_s3client();
    let name = get_new_bucket_name();

    let topic_arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    let bucket = get_new_bucket_name();
    s3_alt.create_bucket().bucket(&bucket).send().await.unwrap();

    let config = aws_sdk_s3::types::NotificationConfiguration::builder()
        .topic_configurations(
            aws_sdk_s3::types::TopicConfiguration::builder()
                .id("id")
                .topic_arn(&topic_arn)
                .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                .build()
                .unwrap(),
        )
        .build();

    // expect AccessDenied because no resource policy allows cross-account access
    let result = s3_alt
        .put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(config.clone())
        .send()
        .await;
    assert!(result.is_err(), "expected AccessDenied for cross-account publish");

    // get alt user's ARN
    let alt_iam = s3_tests_rs::client::get_iam_alt_root_client();
    let alt_user = alt_iam.get_user().send().await.unwrap();
    let alt_principal = alt_user.user().unwrap().arn();

    // add topic policy to allow the alt user
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": alt_principal},
            "Action": "sns:Publish",
            "Resource": &topic_arn
        }]
    });
    sns.set_topic_attributes()
        .topic_arn(&topic_arn)
        .attribute_name("Policy")
        .attribute_value(policy.to_string())
        .send()
        .await
        .unwrap();

    // now cross-account publish should succeed
    s3_alt
        .put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(config)
        .send()
        .await
        .unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}
