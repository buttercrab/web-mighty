use crate::actor::{self, room, room_ss, hub, RoomId};
use crate::util::ExAddr;
use actix::prelude::*;
use std::time::SystemTime;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum UserStatus {
    Online,
    Absent,
    Disconnected,
    Offline,
}

pub struct User {
    no: u32,
    id: String,
    name: String,
    status: UserStatus,
    last_move: SystemTime,
    last_connected: SystemTime,
    room_session: ExAddr<room_ss::RoomSession>,
    room_id: RoomId,
    room: ExAddr<room::Room>,
    server: Addr<hub::Hub>,
}

impl Actor for User {
    type Context = Context<Self>;
}

#[derive(Clone, Message)]
#[rtype(result = "()")]
pub enum Connect {
    Game(Addr<room_ss::RoomSession>, RoomId),
}

impl Handler<Connect> for User {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Self::Context) -> Self::Result {
        match msg {
            Connect::Game(addr, room_id) => {
                self.room_session.set_addr(addr);
                self.room_id = room_id;
                self.status = UserStatus::Online;
                self.last_move = SystemTime::now();
            }
        }
    }
}

#[derive(Clone, Message)]
#[rtype(result = "()")]
pub enum Disconnect {
    Game,
}

impl Handler<Disconnect> for User {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Disconnect::Game => {
                self.room_session.unset_addr();
                self.set_status(UserStatus::Disconnected);
                self.last_connected = SystemTime::now();
                let last = self.last_connected;
                ctx.run_later(actor::RECONNECTION_TIME, move |act, ctx| {
                    if act.last_connected == last && !act.room_session.is_set() {
                        act.set_status(UserStatus::Offline);
                    }
                });
            }
        }
    }
}

#[derive(Clone, Message)]
#[rtype(result = "()")]
pub struct Leave;

impl Handler<Leave> for User {
    type Result = ();

    fn handle(&mut self, msg: Leave, _: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl User {
    pub fn get_status(&self) -> UserStatus {
        match self.status {
            UserStatus::Online => {
                if self.last_move.elapsed().unwrap() >= actor::RECONNECTION_TIME {
                    UserStatus::Absent
                } else {
                    UserStatus::Online
                }
            }
            x => x,
        }
    }

    fn set_status(&mut self, status: UserStatus) {
        self.status = status;
    }
}
