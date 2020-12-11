pub mod api;
pub mod ws;

use crate::app_state::AppState;
use crate::db;
use crate::db::user::{get_info, GetInfoForm};
use actix_identity::Identity;
use actix_web::{get, http, web, Error, HttpResponse, Responder};
use deadpool_postgres::Pool;
use serde_json::{json, Map};

#[get("/admin")]
pub async fn admin(id: Identity, data: web::Data<AppState>, db_pool: web::Data<Pool>) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        if db::user::get_info(db::user::GetInfoForm::UserNo(user_no), (**db_pool).clone())
            .await?
            .is_admin
        {
            let handlebars = data.get_handlebars();
            let body = handlebars.render("admin.hbs", &json!({ "id": id })).unwrap();
            Ok(HttpResponse::Ok().body(body))
        } else {
            Ok(HttpResponse::NotFound().finish())
        }
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[get("/")]
pub async fn index(id: Identity, data: web::Data<AppState>) -> impl Responder {
    if let Some(id) = id.identity() {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("main.hbs", &json!({ "id": id })).unwrap();
        HttpResponse::Ok().body(body)
    } else {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("index.hbs", &json!({})).unwrap();
        HttpResponse::Ok().body(body)
    }
}

#[get("/join/{room_id}")]
pub async fn join(id: Identity, data: web::Data<AppState>, web::Path(room_id): web::Path<String>) -> impl Responder {
    if let Some(id) = id.identity() {
        // todo
        HttpResponse::Ok().body("")
    } else {
        HttpResponse::Found()
            .header(http::header::LOCATION, format!("/login?back=join_{}", room_id))
            .finish()
    }
}

#[get("/list")]
pub async fn list(id: Identity, data: web::Data<AppState>) -> impl Responder {
    if let Some(id) = id.identity() {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("list.hbs", &json!({ "id": id })).unwrap();
        HttpResponse::Ok().body(body)
    } else {
        HttpResponse::Found()
            .header(http::header::LOCATION, "/login?back=list")
            .finish()
    }
}

#[get("/login")]
pub async fn login(id: Identity, data: web::Data<AppState>) -> impl Responder {
    if id.identity().is_some() {
        HttpResponse::Found().header(http::header::LOCATION, "/").finish()
    } else {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("login.hbs", &json!({})).unwrap();
        HttpResponse::Ok().body(body)
    }
}

#[get("/mail/{token}")]
pub async fn mail(data: web::Data<AppState>, web::Path(token): web::Path<String>) -> impl Responder {
    let handlebars = data.get_handlebars();
    let body = handlebars.render("mail.hbs", &json!({})).unwrap();
    HttpResponse::Ok().body(body)
}

#[get("/observe/{room_id}")]
pub async fn observe(id: Identity, data: web::Data<AppState>, web::Path(room_id): web::Path<String>) -> impl Responder {
    let mut val = Map::new();
    if let Some(id) = id.identity() {
        val.insert("id".to_owned(), json!(id));
    }

    let handlebars = data.get_handlebars();
    let body = handlebars.render("observe.hbs", &val).unwrap();
    HttpResponse::Ok().body(body)
}

#[get("/ranking")]
pub async fn ranking(data: web::Data<AppState>) -> impl Responder {
    let handlebars = data.get_handlebars();
    let body = handlebars.render("ranking.hbs", &json!({})).unwrap();
    HttpResponse::Ok().body(body)
}

#[get("/register")]
pub async fn register(id: Identity, data: web::Data<AppState>) -> impl Responder {
    if id.identity().is_some() {
        HttpResponse::Found().header(http::header::LOCATION, "/").finish()
    } else {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("register.hbs", &json!({})).unwrap();
        HttpResponse::Ok().body(body)
    }
}

#[get("/res/{file:.*}")]
pub async fn resource(data: web::Data<AppState>, web::Path(file): web::Path<String>) -> impl Responder {
    let resources = data.get_resources();
    if let Some(body) = resources.get(&file) {
        HttpResponse::Ok().body(body)
    } else {
        HttpResponse::NotFound().finish()
    }
}

#[get("/room/{room_id}")]
pub async fn room(id: Identity, data: web::Data<AppState>, web::Path(room_id): web::Path<String>) -> impl Responder {
    if let Some(id) = id.identity() {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("room.hbs", &json!({ "id": id })).unwrap();
        HttpResponse::Ok().body(body)
    } else {
        HttpResponse::Found()
            .header(http::header::LOCATION, format!("/login?back=room_{}", room_id))
            .finish()
    }
}

#[get("/setting")]
pub async fn setting(id: Identity, data: web::Data<AppState>) -> impl Responder {
    if let Some(id) = id.identity() {
        let handlebars = data.get_handlebars();
        let body = handlebars.render("setting.hbs", &json!({ "id": id })).unwrap();
        HttpResponse::Ok().body(body)
    } else {
        HttpResponse::Found()
            .header(http::header::LOCATION, "/login?back=setting".to_owned())
            .finish()
    }
}

#[get("/user/{user_id}")]
pub async fn user(
    id: Identity,
    data: web::Data<AppState>,
    web::Path(user_id): web::Path<String>,
    db_pool: web::Data<Pool>,
) -> Result<HttpResponse, Error> {
    let mut val = Map::new();
    if let Some(id) = id.identity() {
        val.insert("id".to_owned(), json!(id));
    }

    let user_info = get_info(GetInfoForm::UserId(user_id), (**db_pool).clone()).await?;
    val.insert("user_id".to_owned(), json!(user_info.user_id));
    val.insert("name".to_owned(), json!(user_info.name));
    val.insert("rating".to_owned(), json!(user_info.rating));
    val.insert("is_admin".to_owned(), json!(user_info.is_admin));

    let handlebars = data.get_handlebars();
    let body = handlebars.render("user.hbs", &val).unwrap();
    Ok(HttpResponse::Ok().body(body))
}
