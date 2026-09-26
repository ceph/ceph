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

/* --- Kafka delivery tests --- */

#[cfg(feature = "has_kafka")]
mod kafka_tests {
    use super::*;
    use aws_sdk_s3::primitives::ByteStream;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::config::ClientConfig;
    use rdkafka::Message;
    use std::time::Duration;
    use tokio::time::timeout;

    fn get_kafka_broker() -> String {
        let cfg = s3_tests_rs::config::get_config();
        cfg.kafka_broker.clone()
            .expect("kafka_broker not configured — run kafka-vstart.sh or set [kafka] broker in s3tests.conf")
    }

    fn make_consumer(broker: &str, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", &format!("s3tests-{}", rand::random::<u32>()))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create Kafka consumer");
        consumer.subscribe(&[topic])
            .expect("failed to subscribe to topic");
        consumer
    }

    async fn wait_for_event(consumer: &StreamConsumer, key: &str, event_prefix: &str) -> serde_json::Value {
        timeout(Duration::from_secs(10), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    let obj_key = record["s3"]["object"]["key"]
                                        .as_str().unwrap_or("");
                                    let event_name = record["eventName"]
                                        .as_str().unwrap_or("");
                                    if obj_key == key && event_name.starts_with(event_prefix) {
                                        return event;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await.expect("timed out waiting for Kafka notification")
    }

    #[tokio::test]
    async fn test_notification_kafka_basic() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-test")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-kafka-object";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"hello kafka"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;

        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-delete")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let obj_key = "test-kafka-delete";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"delete me"))
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.delete_object()
            .bucket(&bucket)
            .key(obj_key)
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectRemoved:").await;

        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    async fn setup_notification_with_ack(
        sns: &aws_sdk_sns::Client,
        s3: &aws_sdk_s3::Client,
        broker: &str,
        event: aws_sdk_s3::types::Event,
        ack_level: &str,
    ) -> (String, String, String) {
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", ack_level)
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-notif")
                            .topic_arn(&topic_arn)
                            .events(event)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        (topic_name, bucket, topic_arn)
    }

    async fn setup_notification(
        sns: &aws_sdk_sns::Client,
        s3: &aws_sdk_s3::Client,
        broker: &str,
        event: aws_sdk_s3::types::Event,
    ) -> (String, String, String) {
        setup_notification_with_ack(sns, s3, broker, event, "broker").await
    }

    #[tokio::test]
    async fn test_notification_kafka_event_fields() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-fields/subdir/obj.txt";
        let body = b"event field validation body";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(body))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        let record = &msg["Records"][0];

        assert_eq!(record["eventVersion"].as_str().unwrap(), "2.2");
        assert_eq!(record["eventSource"].as_str().unwrap(), "ceph:s3");
        assert!(record["eventName"].as_str().unwrap().starts_with("s3:ObjectCreated:"));
        assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert_eq!(record["s3"]["object"]["size"].as_u64().unwrap(), body.len() as u64);
        assert!(!record["s3"]["object"]["eTag"].as_str().unwrap_or("").is_empty());
        assert!(!record["userIdentity"]["principalId"].as_str().unwrap_or("").is_empty());
        assert!(!record["requestParameters"]["sourceIPAddress"].as_str().unwrap_or("").is_empty()
            || record["userIdentity"]["principalId"].as_str().is_some());

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_copy() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let src_key = "copy-source";
        s3.put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"copy source data"))
            .send().await.unwrap();

        let _ = wait_for_event(&consumer, src_key, "s3:ObjectCreated:").await;

        let dst_key = "copy-dest";
        s3.copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, dst_key, "s3:ObjectCreated:").await;
        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), dst_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multi_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved,
        ).await;

        let keys = ["multi-del-1", "multi-del-2", "multi-del-3"];
        for key in &keys {
            s3.put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"x"))
                .send().await.unwrap();
        }

        let consumer = make_consumer(&broker, &topic_name);

        let objects: Vec<_> = keys.iter().map(|k|
            aws_sdk_s3::types::ObjectIdentifier::builder().key(*k).build().unwrap()
        ).collect();
        s3.delete_objects()
            .bucket(&bucket)
            .delete(aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build().unwrap())
            .send().await.unwrap();

        let mut received_keys = std::collections::HashSet::new();
        for _ in 0..keys.len() {
            let msg = timeout(Duration::from_secs(10), async {
                loop {
                    match consumer.recv().await {
                        Ok(m) => {
                            if let Some(payload) = m.payload() {
                                if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                    if let Some(record) = event.get("Records")
                                        .and_then(|r| r.as_array())
                                        .and_then(|a| a.first())
                                    {
                                        if record["eventName"].as_str().unwrap_or("").starts_with("s3:ObjectRemoved:") {
                                            return record["s3"]["object"]["key"].as_str().unwrap().to_string();
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }).await.expect("timed out waiting for multi-delete notification");
            received_keys.insert(msg);
        }

        for key in &keys {
            assert!(received_keys.contains(*key), "missing delete event for {key}");
        }

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_no_match() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("should-not-notify")
            .body(ByteStream::from_static(b"no event expected"))
            .send().await.unwrap();

        let result = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if event.get("Records").is_some() {
                                    return event;
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;

        assert!(result.is_err(), "received unexpected notification for non-matching event type");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multipart() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-multipart-notify";
        let mpu = s3.create_multipart_upload()
            .bucket(&bucket)
            .key(obj_key)
            .send().await.unwrap();
        let upload_id = mpu.upload_id().unwrap();

        let part = s3.upload_part()
            .bucket(&bucket)
            .key(obj_key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"multipart notification test data"))
            .send().await.unwrap();

        s3.complete_multipart_upload()
            .bucket(&bucket)
            .key(obj_key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .parts(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().unwrap())
                            .build(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert!(record["s3"]["object"]["size"].as_u64().unwrap() > 0);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_prefix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("prefix-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Prefix)
                                                    .value("logs/")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("data/no-match.txt")
            .body(ByteStream::from_static(b"should not notify"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("logs/access.log")
            .body(ByteStream::from_static(b"should notify"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "logs/access.log", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "logs/access.log");

        let no_match = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("data/no-match.txt") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(no_match.is_err(), "received notification for non-matching prefix");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multiple_topics() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let create_topic = get_new_bucket_name();
        let delete_topic = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let create_arn = sns.create_topic()
            .name(&create_topic)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        let delete_arn = sns.create_topic()
            .name(&delete_topic)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("creates")
                            .topic_arn(&create_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .build()
                            .unwrap(),
                    )
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("deletes")
                            .topic_arn(&delete_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let create_consumer = make_consumer(&broker, &create_topic);
        let delete_consumer = make_consumer(&broker, &delete_topic);

        let obj_key = "multi-topic-obj";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"hello"))
            .send().await.unwrap();

        let msg = wait_for_event(&create_consumer, obj_key, "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let msg = wait_for_event(&delete_consumer, obj_key, "s3:ObjectRemoved:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_cross_account_isolation() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let sns_alt = get_sns_alt_root_client();
        let s3 = get_iam_root_s3client();
        let s3_alt = get_iam_alt_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let (_, alt_bucket_name, _) = setup_notification(
            &sns_alt, &s3_alt, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        s3_alt.put_object()
            .bucket(&alt_bucket_name)
            .key("alt-obj")
            .body(ByteStream::from_static(b"alt account data"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("my-obj")
            .body(ByteStream::from_static(b"my data"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "my-obj", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["bucket"]["name"].as_str().unwrap(), bucket);

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("alt-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event from alt account's bucket on our topic");

        nuke_topics(&sns, &get_topic_prefix()).await;
        nuke_topics(&sns_alt, &get_topic_prefix()).await;
    }

    /* --- ack-level=none (best-effort / fire-and-forget) --- */

    #[tokio::test]
    async fn test_notification_kafka_ack_none_basic() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated, "none",
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "ack-none-basic";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"fire and forget"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_ack_none_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved, "none",
        ).await;

        let obj_key = "ack-none-delete";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"will delete"))
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectRemoved:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_ack_none_multi_object() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated, "none",
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let keys = ["none-obj-1", "none-obj-2", "none-obj-3", "none-obj-4", "none-obj-5"];
        for key in &keys {
            s3.put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"x"))
                .send().await.unwrap();
        }

        let mut received = std::collections::HashSet::new();
        for _ in 0..keys.len() {
            let key = timeout(Duration::from_secs(10), async {
                loop {
                    match consumer.recv().await {
                        Ok(m) => {
                            if let Some(payload) = m.payload() {
                                if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                    if let Some(record) = event.get("Records")
                                        .and_then(|r| r.as_array())
                                        .and_then(|a| a.first())
                                    {
                                        if record["eventName"].as_str().unwrap_or("").starts_with("s3:ObjectCreated:") {
                                            return record["s3"]["object"]["key"].as_str().unwrap().to_string();
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }).await.expect("timed out waiting for ack=none notification");
            received.insert(key);
        }

        for key in &keys {
            assert!(received.contains(*key), "missing ack=none event for {key}");
        }

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- suffix filter --- */

    #[tokio::test]
    async fn test_notification_kafka_filter_suffix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("suffix-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Suffix)
                                                    .value(".jpg")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("readme.txt")
            .body(ByteStream::from_static(b"no match"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("photo.jpg")
            .body(ByteStream::from_static(b"match"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "photo.jpg", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "photo.jpg");

        let no_match = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("readme.txt") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(no_match.is_err(), "received notification for non-matching suffix");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- all events on single topic --- */

    #[tokio::test]
    async fn test_notification_kafka_all_events() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("all-events")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "all-events-obj";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"create then delete"))
            .send().await.unwrap();

        let create_msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        assert_eq!(create_msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let delete_msg = wait_for_event(&consumer, obj_key, "s3:ObjectRemoved:").await;
        assert_eq!(delete_msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- overwrite, large object, notification CRUD, combined filters, topic attrs --- */

    #[tokio::test]
    async fn test_notification_kafka_overwrite() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "overwrite-me";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"version 1"))
            .send().await.unwrap();

        let msg1 = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        let size1 = msg1["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size1, 9);

        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"version 2 with more data"))
            .send().await.unwrap();

        let msg2 = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        let size2 = msg2["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size2, 24);
        assert_ne!(
            msg1["Records"][0]["s3"]["object"]["eTag"].as_str(),
            msg2["Records"][0]["s3"]["object"]["eTag"].as_str(),
            "overwrite should produce different etag"
        );

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_large_object() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "large-object";
        let body = vec![0x42u8; 1024 * 1024]; // 1 MB
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from(body))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "s3:ObjectCreated:").await;
        let size = msg["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size, 1024 * 1024, "event should report exact object size");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_config_crud() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, topic_arn) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let config = s3.get_bucket_notification_configuration()
            .bucket(&bucket)
            .send().await.unwrap();
        let topics = config.topic_configurations();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic_arn(), &topic_arn);

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder().build(),
            )
            .send().await.unwrap();

        let config = s3.get_bucket_notification_configuration()
            .bucket(&bucket)
            .send().await.unwrap();
        assert!(config.topic_configurations().is_empty(),
            "cleared notification config should have no topics");

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("after-clear")
            .body(ByteStream::from_static(b"no notification"))
            .send().await.unwrap();

        let result = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if event.get("Records").is_some() {
                                    return event;
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(result.is_err(), "should not receive event after clearing notification config");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_prefix_and_suffix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("combo-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Prefix)
                                                    .value("images/")
                                                    .build(),
                                            )
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Suffix)
                                                    .value(".png")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        // prefix match but wrong suffix — should not fire
        s3.put_object()
            .bucket(&bucket)
            .key("images/photo.jpg")
            .body(ByteStream::from_static(b"wrong suffix"))
            .send().await.unwrap();

        // right suffix but wrong prefix — should not fire
        s3.put_object()
            .bucket(&bucket)
            .key("docs/diagram.png")
            .body(ByteStream::from_static(b"wrong prefix"))
            .send().await.unwrap();

        // both match — should fire
        s3.put_object()
            .bucket(&bucket)
            .key("images/icon.png")
            .body(ByteStream::from_static(b"match"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "images/icon.png", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "images/icon.png");

        // verify the non-matching ones didn't arrive
        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    let key = record["s3"]["object"]["key"].as_str().unwrap_or("");
                                    if key == "images/photo.jpg" || key == "docs/diagram.png" {
                                        return key.to_string();
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching prefix+suffix combo");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_topic_attributes() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        let attrs = sns.get_topic_attributes()
            .topic_arn(&topic_arn)
            .send().await.unwrap();

        let attr_map = attrs.attributes().unwrap();
        assert_eq!(attr_map.get("TopicArn").map(|s| s.as_str()), Some(topic_arn.as_str()));

        let push_ep = attr_map.get("push-endpoint")
            .or_else(|| attr_map.get("EndPoint"));
        assert!(push_ep.is_some(), "topic attributes should include push endpoint");
        if let Some(ep) = push_ep {
            assert!(ep.contains("kafka://"), "endpoint should be kafka URL, got: {ep}");
        }

        sns.set_topic_attributes()
            .topic_arn(&topic_arn)
            .attribute_name("Policy")
            .attribute_value(r#"{"Version":"2012-10-17","Statement":[]}"#)
            .send().await.unwrap();

        let attrs2 = sns.get_topic_attributes()
            .topic_arn(&topic_arn)
            .send().await.unwrap();
        let policy = attrs2.attributes().unwrap().get("Policy");
        assert!(policy.is_some(), "Policy attribute should be set");

        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- metadata and tag filters --- */

    /* The AWS SDK only generates <S3Key> filter rules. RGW's metadata
     * and tag filter extensions use <S3Metadata> and <S3Tags> elements.
     * This interceptor rewrites the PutBucketNotificationConfiguration
     * XML body to move x-amz-meta-* rules into <S3Metadata> and
     * non-key rules into <S3Tags>. */
    #[derive(Debug, Clone)]
    struct NotificationFilterRewriter;

    impl aws_smithy_runtime_api::client::interceptors::Intercept for NotificationFilterRewriter {
        fn name(&self) -> &'static str { "NotificationFilterRewriter" }

        fn modify_before_signing(
            &self,
            context: &mut aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut<'_>,
            _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
            _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
        ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
            let request = context.request_mut();
            let body_bytes = request.body().bytes()
                .ok_or("no body")?
                .to_vec();
            let body_str = String::from_utf8(body_bytes)?;

            if !body_str.contains("NotificationConfiguration") {
                return Ok(());
            }

            let mut result = body_str.clone();

            /* extract filter rules that belong in S3Metadata or S3Tags,
             * move them from <S3Key> into the correct sibling element */
            let mut metadata_rules = Vec::new();
            let mut tag_rules = Vec::new();

            /* find all FilterRule elements and classify them */
            let mut remaining_key_rules = String::new();
            let mut pos = 0;
            while let Some(start) = result[pos..].find("<FilterRule>") {
                let abs_start = pos + start;
                if let Some(end) = result[abs_start..].find("</FilterRule>") {
                    let abs_end = abs_start + end + "</FilterRule>".len();
                    let rule = &result[abs_start..abs_end];

                    if let (Some(ns), Some(ne)) = (rule.find("<Name>"), rule.find("</Name>")) {
                        let name = &rule[ns + 6..ne];
                        if let (Some(vs), Some(ve)) = (rule.find("<Value>"), rule.find("</Value>")) {
                            let value = &rule[vs + 7..ve];
                            if name.starts_with("x-amz-meta-") {
                                metadata_rules.push((name.to_string(), value.to_string()));
                                pos = abs_end;
                                continue;
                            } else if name != "prefix" && name != "suffix" {
                                tag_rules.push((name.to_string(), value.to_string()));
                                pos = abs_end;
                                continue;
                            }
                        }
                    }
                    remaining_key_rules.push_str(rule);
                    pos = abs_end;
                } else {
                    break;
                }
            }

            if metadata_rules.is_empty() && tag_rules.is_empty() {
                return Ok(());
            }

            /* rebuild the Filter block with separated elements */
            let mut extra = String::new();
            if !metadata_rules.is_empty() {
                extra.push_str("<S3Metadata>");
                for (k, v) in &metadata_rules {
                    extra.push_str(&format!("<FilterRule><Name>{k}</Name><Value>{v}</Value></FilterRule>"));
                }
                extra.push_str("</S3Metadata>");
            }
            if !tag_rules.is_empty() {
                extra.push_str("<S3Tags>");
                for (k, v) in &tag_rules {
                    extra.push_str(&format!("<FilterRule><Name>{k}</Name><Value>{v}</Value></FilterRule>"));
                }
                extra.push_str("</S3Tags>");
            }

            /* remove the moved rules from <S3Key> and inject the new elements
             * after </S3Key> inside <Filter> */
            let mut new_body = result.clone();
            /* remove all FilterRule elements, then re-add only the key ones */
            if let Some(s3key_start) = new_body.find("<S3Key>") {
                if let Some(s3key_end) = new_body.find("</S3Key>") {
                    let end_pos = s3key_end + "</S3Key>".len();
                    let before = &new_body[..s3key_start];
                    let after = &new_body[end_pos..];
                    if remaining_key_rules.is_empty() {
                        new_body = format!("{before}{extra}{after}");
                    } else {
                        new_body = format!("{before}<S3Key>{remaining_key_rules}</S3Key>{extra}{after}");
                    }
                }
            }

            eprintln!("NotificationFilterRewriter: rewritten body:\n{}", new_body);
            let new_len = new_body.len();
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(new_body);
            request.headers_mut().insert("content-length", new_len.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_metadata() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("meta-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::from("x-amz-meta-color"))
                                                    .value("blue")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .customize()
            .interceptor(NotificationFilterRewriter)
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("red-obj")
            .metadata("color", "red")
            .body(ByteStream::from_static(b"wrong metadata"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("blue-obj")
            .metadata("color", "blue")
            .body(ByteStream::from_static(b"matching metadata"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "blue-obj", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "blue-obj");

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("red-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching metadata value");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_tags() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("tag-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::from("environment"))
                                                    .value("production")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .customize()
            .interceptor(NotificationFilterRewriter)
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("staging-obj")
            .tagging("environment=staging")
            .body(ByteStream::from_static(b"wrong tag"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("prod-obj")
            .tagging("environment=production")
            .body(ByteStream::from_static(b"matching tag"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "prod-obj", "s3:ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "prod-obj");

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("staging-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching tag value");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }
}
