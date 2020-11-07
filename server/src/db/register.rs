use crate::handlers::RegisterForm;
use actix_web::{HttpResponse, ResponseError};
use deadpool_postgres::{Pool, PoolError};
use derive_more::Display;

#[derive(Debug, Display)]
pub enum RegisterError {
    PoolError(PoolError),
    InvalidUsername,
    UsernameExist,
    InvalidPassword,
}

impl From<PoolError> for RegisterError {
    fn from(e: PoolError) -> Self {
        RegisterError::PoolError(e)
    }
}

impl From<tokio_postgres::Error> for RegisterError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::from(PoolError::from(e))
    }
}

impl ResponseError for RegisterError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            RegisterError::PoolError(ref err) => HttpResponse::InternalServerError().body(err.to_string()),
            RegisterError::InvalidUsername => HttpResponse::BadRequest().body("username not allowed"),
            RegisterError::UsernameExist => HttpResponse::Conflict().body("username exists"),
            RegisterError::InvalidPassword => HttpResponse::BadRequest().body("password is not allowed"),
        }
    }
}

// todo: change sql
pub async fn register(form: &RegisterForm, pool: &Pool) -> Result<(), RegisterError> {
    let client = pool.get().await?;
    let stmt = client.prepare("SELECT id FROM user WHERE id=$1").await?;
    let res = client.query(&stmt, &[&form.username]).await?;
    if !res.is_empty() {
        return Err(RegisterError::UsernameExist);
    }

    let client = pool.get().await?;
    let stmt = client.prepare("INSERT ...").await?;
    let _ = client.query(&stmt, &[&form.username, &form.password_hash]).await?;
    Ok(())
}
