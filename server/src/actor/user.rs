use crate::actor::hub::GetRoom;
use crate::actor::room::{ChangeName, ChangeRule, Chat, Go, RoomJoin, RoomLeave, StartGame};
use crate::actor::session::Session;
use crate::actor::{Hub, Main, Room, RoomUser};
use crate::db::game::{change_rating, ChangeRatingForm};
use crate::dev::*;
use actix::prelude::*;
use mighty::prelude::State;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

/// Joined Room Information
#[derive(Debug)]
pub struct JoinedRoom {
    addr: Addr<Room>,
    info: RoomInfo,
    group: HashSet<Addr<Session<RoomUser>>>,
    disconn: u8,
}

/// User Actor
///
/// Main & Room connections connect to this actor
#[derive(Debug)]
pub struct User {
    info: UserInfo,
    status: UserStatus,
    conn: u8,
    disconn: u8,
    last_update: SystemTime,
    room: Option<JoinedRoom>,
    subscribers: HashSet<Addr<Session<Main>>>,
    hub: Addr<Hub>,
    pool: Pool,
}

impl Actor for User {
    type Context = Context<Self>;
}

/// Connects to user
///
/// `Subscribe` is for status subscription for other user.
#[derive(Debug, Clone, Message)]
#[rtype(result = "Result<()>")]
pub enum UserConnect {
    Room(Addr<Session<RoomUser>>),
    Subscribe(Addr<Session<Main>>),
    Main,
}

impl Handler<UserConnect> for User {
    type Result = Result<()>;

    fn handle(&mut self, msg: UserConnect, _: &mut Self::Context) -> Self::Result {
        match msg {
            UserConnect::Room(addr) => {
                ensure!(self.room.is_some(), StatusCode::BAD_REQUEST, "no joined room");
                self.room.as_mut().unwrap().group.insert(addr);
            }
            UserConnect::Subscribe(addr) => {
                addr.do_send(MainToClient::UserStatus(self.info.no, self.status));
                self.subscribers.insert(addr);
            }
            UserConnect::Main => {
                self.conn += 1;
                self.update_status();
            }
        }
        Ok(())
    }
}

/// Disconnects to user
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub enum UserDisconnect {
    Room(Addr<Session<RoomUser>>),
    Unsubscribe(Addr<Session<Main>>),
    Main,
}

impl Handler<UserDisconnect> for User {
    type Result = ();

    fn handle(&mut self, msg: UserDisconnect, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            UserDisconnect::Room(addr) => {
                if self.room.is_none() {
                    return;
                }
                let room = self.room.as_mut().unwrap();
                room.group.remove(&addr);
                room.disconn += 1;
                self.update_status();
                ctx.run_later(RECONNECTION_TIME, |act, _| {
                    act.room.as_mut().unwrap().disconn -= 1;
                    act.update_status();
                });
            }
            UserDisconnect::Unsubscribe(addr) => {
                self.subscribers.remove(&addr);
            }
            UserDisconnect::Main => {
                self.conn -= 1;
                self.disconn += 1;
                self.update_status();
                ctx.run_later(RECONNECTION_TIME, |act, _| {
                    act.disconn -= 1;
                    act.update_status();
                });
            }
        }
    }
}

/// Joins to the room
/// It would respond `room_info` so that the client can know if the client is joined.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct UserJoin(pub RoomId, pub Addr<Session<Main>>);

impl Handler<UserJoin> for User {
    type Result = ();

    fn handle(&mut self, msg: UserJoin, ctx: &mut Self::Context) -> Self::Result {
        if self.room.is_some() {
            return;
        }
        self.hub
            .send(GetRoom(msg.0))
            .into_actor(self)
            .then(|res, act, ctx| {
                if let Ok(Ok(room)) = res {
                    let room_addr = room.clone();
                    room.send(RoomJoin::User(act.info.no, ctx.address()))
                        .into_actor(act)
                        .then(move |res, act, _| {
                            if let Ok(info) = res {
                                if info.user.contains(&act.info.no) {
                                    act.room = Some(JoinedRoom {
                                        addr: room_addr,
                                        info,
                                        group: HashSet::new(),
                                        disconn: 0,
                                    });
                                } else {
                                    msg.1.do_send(MainToClient::UserInfo(act.info.clone()));
                                }
                            }

                            fut::ready(())
                        })
                        .wait(ctx);
                }

                fut::ready(())
            })
            .wait(ctx);
    }
}

/// Leaves the room
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct UserLeave;

impl Handler<UserLeave> for User {
    type Result = ();

    fn handle(&mut self, _: UserLeave, _: &mut Self::Context) -> Self::Result {
        if self.room.is_none() {
            return;
        }
        self.room.as_ref().unwrap().addr.do_send(RoomLeave::User(self.info.no));
    }
}

/// Commands that user sent
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct UserCommand(pub RoomUserToServer);

impl Handler<UserCommand> for User {
    type Result = ();

    fn handle(&mut self, msg: UserCommand, _: &mut Self::Context) -> Self::Result {
        if self.room.is_none() {
            return;
        }
        let room = self.room.as_ref().unwrap();
        let user_no = self.info.no;
        match msg.0 {
            RoomUserToServer::Start => {
                room.addr.do_send(StartGame(user_no));
            }
            RoomUserToServer::ChangeName(name) => {
                room.addr.do_send(ChangeName(user_no, name));
            }
            RoomUserToServer::ChangeRule(rule) => {
                room.addr.do_send(ChangeRule(user_no, rule));
            }
            RoomUserToServer::Command(cmd) => {
                room.addr.do_send(Go(user_no, cmd));
            }
            RoomUserToServer::Chat(chat) => {
                room.addr.do_send(Chat::User(chat, self.info.no));
            }
        }
    }
}

/// Request for room info
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct GotRoomInfo(pub RoomInfo);

impl Handler<GotRoomInfo> for User {
    type Result = ();

    fn handle(&mut self, msg: GotRoomInfo, _: &mut Self::Context) -> Self::Result {
        if self.room.is_none() {
            return;
        }
        let room = self.room.as_mut().unwrap();
        for i in room.group.iter() {
            i.do_send(RoomUserToClient::Room(msg.0.clone()));
        }
        room.info = msg.0;
    }
}

/// Passing game state to user
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct GotGameState(pub State);

impl Handler<GotGameState> for User {
    type Result = ();

    fn handle(&mut self, msg: GotGameState, _: &mut Self::Context) -> Self::Result {
        if self.room.is_none() {
            return;
        }
        for i in self.room.as_ref().unwrap().group.iter() {
            i.do_send(RoomUserToClient::Game(msg.0.clone()));
        }
    }
}

/// Update for absent
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct Update;

impl Handler<Update> for User {
    type Result = ();

    fn handle(&mut self, _: Update, ctx: &mut Self::Context) -> Self::Result {
        self.last_update = SystemTime::now();
        self.update_status();
        ctx.run_later(ABSENT_TIME + Duration::from_millis(1), |act, _| {
            act.update_status();
        });
    }
}

/// change
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct ChangeRating(pub i32, pub GameId);

impl Handler<ChangeRating> for User {
    type Result = ();

    fn handle(&mut self, msg: ChangeRating, _: &mut Self::Context) -> Self::Result {
        if msg.0 < 0 {
            if (-msg.0) as u32 > self.info.rating {
                self.info.rating = 0;
            } else {
                self.info.rating -= (-msg.0) as u32;
            }
        } else {
            self.info.rating += msg.0 as u32;
        }
        let form = ChangeRatingForm {
            user_no: self.info.no.0,
            game_id: msg.1,
            diff: msg.0 as u32,
            rating: self.info.rating,
        };
        let _ = change_rating(&form, self.pool.clone());
    }
}

#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct SendChat(pub String, pub UserNo);

impl Handler<SendChat> for User {
    type Result = ();

    fn handle(&mut self, msg: SendChat, _: &mut Self::Context) -> Self::Result {
        if self.room.is_none() {
            return;
        }

        let room = self.room.as_mut().unwrap();
        for i in room.group.iter() {
            i.do_send(RoomUserToClient::Chat(msg.0.clone(), msg.1));
        }
    }
}

impl User {
    pub fn new(info: UserInfo, hub: Addr<Hub>, pool: Pool) -> User {
        User {
            info,
            status: UserStatus::OFFLINE,
            conn: 0,
            disconn: 0,
            last_update: SystemTime::now(),
            room: None,
            subscribers: HashSet::new(),
            hub,
            pool,
        }
    }

    fn get_status(&mut self) -> UserStatus {
        let mut status = UserStatus::empty();
        let mut leave_room = false;

        if let Some(room) = &self.room {
            status |= UserStatus::IN_ROOM;

            if room.info.is_game {
                status |= UserStatus::IN_GAME;
            }

            if room.group.is_empty() {
                if room.disconn > 0 {
                    status |= UserStatus::ROOM_DISCONN;
                } else if room.info.is_game {
                    // todo: ghost
                } else {
                    leave_room = true;
                }
            }
        }

        if leave_room {
            self.room.as_ref().unwrap().addr.do_send(RoomLeave::User(self.info.no));
        }

        if self.conn == 0 {
            if self.disconn == 0 {
                status |= UserStatus::OFFLINE;
            } else {
                status |= UserStatus::DISCONNECTED;
            }
        } else if self.last_update.elapsed().unwrap_or_else(|_| Duration::from_secs(0)) >= ABSENT_TIME {
            status |= UserStatus::ABSENT;
        } else {
            status |= UserStatus::ONLINE;
        }

        status
    }

    fn update_status(&mut self) {
        let new_state = self.get_status();
        if self.status != new_state {
            self.status = new_state;

            for addr in self.subscribers.iter() {
                addr.do_send(MainToClient::UserStatus(self.info.no, self.status));
            }
        }
    }
}
