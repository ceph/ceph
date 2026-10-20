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

/// The result of a *metadata* request S3 permits on an object another
/// identity owns -- its tags or its ACL.
///
/// Which way this goes is the gateway's access model, not the
/// permission check, so it cannot be asked of `admin::fs_access`:
/// under `s3-metadata` the gateway performs the operation itself once
/// S3 has authorised it, and under `posix-strict` the filesystem's
/// denial stands.  `features::cross_account_metadata()` reports
/// which, by interrogating the gateway.
///
/// `None` -- the endpoint reported no model and none was declared --
/// asserts **nothing**, because a suite that guesses here asserts the
/// opposite of the truth against half the backends it meets.
#[macro_export]
macro_rules! unless_posix_governs_metadata {
    ($model:expr, $result:expr) => {{
        let result = $result;
        match $model {
            Some(true) => Some(result.unwrap()),
            Some(false) => {
                $crate::assert_s3_err!(result, 403, "AccessDenied");
                None
            }
            None => result.ok(),
        }
    }};
}

/// The result of a *data* request S3 permits on an object another
/// identity owns -- anything that moves its bytes.
///
/// Two inputs, because both can deny it.  Under a model where POSIX
/// governs data the permission check decides, so this defers to
/// `unless_fs_denies!`;  under `s3-native` S3 is the authority for
/// bytes as well and the gateway serves what POSIX denied, so the
/// check says nothing.  `features::cross_account_data()` reports
/// which, and the caller passes it:  a helper that read the model
/// itself would assert the strict models' behaviour against whatever
/// model the run configured.
///
/// `None` -- the endpoint reported no model and none was declared --
/// asserts **nothing**, as in `unless_posix_governs_metadata!`.
#[macro_export]
macro_rules! unless_posix_governs_data {
    ($model:expr, $check:expr, $result:expr) => {{
        let result = $result;
        match $model {
            Some(true) => Some(result.unwrap()),
            Some(false) => $crate::unless_fs_denies!($check, result),
            None => result.ok(),
        }
    }};
}

/// The result of a request S3 permits, where the filesystem may still
/// deny it:  under impersonation nsfs serves each request as its POSIX
/// identity, and the filesystem decides an object's content too
/// (`admin::fs_access`).  AccessDenied where `check` denied it;  the
/// output, unwrapped, otherwise -- `Some` in that case, so a test checks
/// what it read only where it read.
///
/// The primitive under `unless_posix_governs_data!`, which is what a
/// test asserting a cross-identity data denial should reach for:  this
/// one trusts the check whatever the access model says.
#[macro_export]
macro_rules! unless_fs_denies {
    ($check:expr, $result:expr) => {{
        let result = $result;
        if $check.denied() {
            $crate::assert_s3_err!(result, 403, "AccessDenied");
            None
        } else {
            Some(result.unwrap())
        }
    }};
}
