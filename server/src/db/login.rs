use crate::handlers::LoginForm;
use actix_web::{HttpResponse, ResponseError};
use deadpool_postgres::{Pool, PoolError};
use derive_more::Display;

#[derive(Debug, Display)]
pub enum LoginError {
    PoolError(PoolError),
    NoUser,
    WrongPassword,
}

impl From<PoolError> for LoginError {
    fn from(e: PoolError) -> Self {
        LoginError::PoolError(e)
    }
}

impl From<tokio_postgres::Error> for LoginError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::from(PoolError::from(e))
    }
}

impl ResponseError for LoginError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            LoginError::PoolError(ref err) => {
                HttpResponse::InternalServerError().body(err.to_string())
            }
            LoginError::NoUser => HttpResponse::Unauthorized().body("no user found"),
            LoginError::WrongPassword => HttpResponse::Unauthorized().body("incorrect password"),
        }
    }
}

// todo: change sql
pub async fn login(form: &LoginForm, pool: &Pool) -> Result<(), LoginError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare("SELECT password FROM user WHERE id=$1")
        .await?;
    let res = client.query(&stmt, &[&form.username]).await?;
    if res.is_empty() {
        return Err(LoginError::NoUser);
    }
    let password: String = res[0].get(0);
    if password != form.password_hash {
        return Err(LoginError::WrongPassword);
    }
    Ok(())
}
