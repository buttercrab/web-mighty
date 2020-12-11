use crate::actor::{ChatSession, ListSession, MainSession, ObserveSession, RoomSession, UserNo};
use crate::app_state::AppState;
use actix_identity::Identity;
use actix_web::{get, web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;

#[get("/chat")]
pub async fn chat(
    id: Identity,
    data: web::Data<AppState>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        ws::start(ChatSession::new(UserNo(user_no), data.hub.clone()), &req, stream)
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[get("/list")]
pub async fn list(
    id: Identity,
    data: web::Data<AppState>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        ws::start(ListSession::new(UserNo(user_no), data.hub.clone()), &req, stream)
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[get("/main")]
pub async fn main(
    id: Identity,
    data: web::Data<AppState>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        ws::start(MainSession::new(UserNo(user_no), data.hub.clone()), &req, stream)
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[get("/observe")]
pub async fn observe(
    id: Identity,
    data: web::Data<AppState>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        ws::start(ObserveSession::new(UserNo(user_no), data.hub.clone()), &req, stream)
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[get("/room")]
pub async fn room(
    id: Identity,
    data: web::Data<AppState>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    if let Some(id) = id.identity() {
        let user_no = id.parse::<u32>().map_err(|_| Error::from(()))?;
        ws::start(RoomSession::new(UserNo(user_no), data.hub.clone()), &req, stream)
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}
