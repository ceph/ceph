use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;

#[derive(Debug)]
pub struct S3ErrorInfo {
    pub status: u16,
    pub error_code: String,
    pub message: Option<String>,
}

pub fn extract_s3_error<E>(err: &SdkError<E, Response>) -> S3ErrorInfo
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            let meta = service_err.err();
            let error_code = meta.code().unwrap_or("Unknown").to_string();
            let message = meta.message().map(|s| s.to_string());
            S3ErrorInfo {
                status,
                error_code,
                message,
            }
        }
        other => {
            panic!("Expected ServiceError, got: {other:?}");
        }
    }
}

#[macro_export]
macro_rules! expect_s3_err {
    ($expr:expr) => {
        match $expr {
            Err(ref e) => $crate::error::extract_s3_error(e),
            Ok(_) => panic!("Expected S3 error, but operation succeeded"),
        }
    };
}

#[macro_export]
macro_rules! assert_s3_err {
    ($expr:expr, $status:expr, $code:expr) => {{
        let err_info = $crate::expect_s3_err!($expr);
        assert_eq!(
            err_info.status, $status,
            "Expected HTTP {}, got {}",
            $status, err_info.status
        );
        assert_eq!(
            err_info.error_code, $code,
            "Expected error code {}, got {}",
            $code, err_info.error_code
        );
        err_info
    }};
}
