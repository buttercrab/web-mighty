use crate::actor::hub::GetUser;
use crate::actor::user::{UserConnect, UserDisconnect};
use crate::actor::{Hub, User};
use crate::dev::*;
use actix::prelude::*;
use actix_web_actors::ws::WebsocketContext;
use types::{MainToClient, MainToServer};

pub struct Main {
    user: Addr<User>,
    hub: Addr<Hub>,
    connection: Option<Addr<Connection<Session<Main>>>>,
}

impl SessionTrait for Main {
    type Sender = MainToClient;

    fn started(act: &mut Session<Self>, ctx: &mut WebsocketContext<Session<Self>>) {
        act.inner.user.do_send(UserConnect::Main(ctx.address()));
    }

    fn stopped(act: &mut Session<Self>, ctx: &mut WebsocketContext<Session<Self>>) {
        act.inner.user.do_send(UserDisconnect::Main(ctx.address()));
    }

    fn receive(act: &mut Session<Self>, msg: String, ctx: &mut WebsocketContext<Session<Self>>) {
        let msg: MainToServer = serde_json::from_str(&*msg).unwrap();
        match msg {
            MainToServer::Subscribe(no) => {
                ignore!(ignore!(send(act, ctx, act.inner.hub.clone(), GetUser(no))))
                    .do_send(UserConnect::Subscribe(ctx.address()));
            }
            MainToServer::Unsubscribe(no) => {
                ignore!(ignore!(send(act, ctx, act.inner.hub.clone(), GetUser(no))))
                    .do_send(UserDisconnect::Unsubscribe(ctx.address()));
            }
            _ => {}
        }
        if let Some(connection) = &act.inner.connection {
            connection.do_send(Update);
        }
    }
}

impl Main {
    pub fn new(user: Addr<User>, hub: Addr<Hub>) -> Main {
        Main {
            user,
            hub,
            connection: None,
        }
    }
}
