use serde_json::{json, Value};

pub fn make_json_policy(
    action: &str,
    resource: &str,
    principal: Option<Value>,
    effect: Option<&str>,
    conditions: Option<Value>,
) -> String {
    let principal = principal.unwrap_or(json!({"AWS": "*"}));
    let effect = effect.unwrap_or("Allow");

    let mut statement = json!({
        "Action": action,
        "Principal": principal,
        "Effect": effect,
        "Resource": resource,
    });

    if let Some(cond) = conditions {
        statement["Condition"] = cond;
    }

    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [statement],
    });

    serde_json::to_string(&policy).unwrap()
}

pub fn make_arn_resource(path: &str) -> String {
    format!("arn:aws:s3:::{path}")
}
