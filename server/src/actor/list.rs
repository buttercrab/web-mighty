use crate::actor::room::RoomInfo;
use crate::actor::Hub;
use crate::dev::*;
use actix::prelude::*;
use actix_web_actors::ws::WebsocketContext;
use serde::{Deserialize, Serialize};

pub struct List {
    hub: Addr<Hub>,
}

#[derive(Clone, Message, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum ListSend {
    Room(RoomInfo),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ListReceive;

impl SessionTrait for List {
    type Receiver = ListSend;

    fn receive(_: &mut Session<Self>, msg: String, _: &mut WebsocketContext<Session<Self>>) {
        let _: ListReceive = serde_json::from_str(&*msg).unwrap();
    }
}

impl List {
    pub fn new(hub: Addr<Hub>) -> List {
        List { hub }
    }
}
