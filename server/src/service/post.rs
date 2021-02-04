use crate::app_state::AppState;
use crate::db::user::{login_user, pre_register_user, register_user, LoginForm, PreRegisterForm, RegisterForm, RegenerateTokenForm};
use crate::dev::*;
use actix_identity::Identity;
use actix_web::{http, post, web, HttpResponse, Responder, HttpRequest};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct LoginQuery {
    back: Option<String>,
}

#[post("/login")]
pub async fn login(
    id: Identity,
    form: web::Json<LoginForm>,
    state: web::Data<AppState>,
    query: web::Query<LoginQuery>,
) -> Result<HttpResponse, Error> {
    let user_no = login_user((*form).clone(), state.pool.clone())?;
    id.remember(user_no.to_string());
    Ok(HttpResponse::Found()
        .header(
            http::header::LOCATION,
            query.back.clone().unwrap_or_else(|| "/".to_owned()),
        )
        .finish()
        .into_body())
}

#[post("/logout")]
pub async fn logout(id: Identity) -> impl Responder {
    id.forget();
    HttpResponse::Ok()
}

#[post("/pre-register")]
pub async fn pre_register(form: web::Json<PreRegisterForm>, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let form = pre_register_user((*form).clone(), state.pool.clone())?;
    state.mail.do_send(form);
    Ok(HttpResponse::Found()
        .header(http::header::LOCATION, "/")
        .finish()
        .into_body())
}

#[post("/register")]
pub async fn register(
    id: Identity,
    form: web::Json<RegisterForm>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let _ = register_user((*form).clone(), state.pool.clone())?;
    id.remember(form.user_id.clone());
    Ok(HttpResponse::Found()
        .header(http::header::LOCATION, "/")
        .finish()
        .into_body())
}
