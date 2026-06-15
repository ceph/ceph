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
}
