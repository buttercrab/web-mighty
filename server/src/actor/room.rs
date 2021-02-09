use crate::actor::hub::RemoveRoom;
use crate::actor::session::Session;
use crate::actor::user::{ChangeRating, GotGameState, GotRoomInfo, SendChat};
use crate::actor::{hub, Hub, List, Observe, User};
use crate::db::game::{
    change_room_info, get_into_room, get_rule, leave_room, make_game, save_rule, save_state, ChangeRoomInfoForm,
    GetInRoomForm, GetRuleForm, LeaveRoomForm, MakeGameForm, SaveRuleForm, SaveStateForm,
};
use crate::dev::*;
use actix::prelude::*;
use mighty::prelude::{Command, Game, Rule, State};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Information of game
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameInfo {
    id: GameId,
    no: u32,
    game: Game,
}

/// Room Actor
///
/// This contains all the information for room
#[derive(Debug)]
pub struct Room {
    info: RoomInfo,
    game: Option<GameInfo>,
    user_addr: HashMap<UserNo, Addr<User>>,
    observe: HashSet<Addr<Session<Observe>>>,
    list: HashSet<Addr<Session<List>>>,
    hub: Addr<Hub>,
    pool: Pool,
}

impl Actor for Room {
    type Context = Context<Self>;
}

/// Joins to room
/// This returns RoomInfo for receivers to check if they're joined successfully
#[derive(Debug, Clone, Message)]
#[rtype(result = "RoomInfo")]
pub enum RoomJoin {
    User(UserNo, Addr<User>),
    Observe(Addr<Session<Observe>>),
    List(Addr<Session<List>>),
}

impl Handler<RoomJoin> for Room {
    type Result = RoomInfo;

    fn handle(&mut self, msg: RoomJoin, _: &mut Self::Context) -> Self::Result {
        match msg {
            RoomJoin::User(user_no, addr) => {
                if self.info.is_game {
                    return self.info.clone();
                }
                let mut is_full = true;
                for i in self.info.user.iter_mut() {
                    if i.0 == 0 {
                        *i = user_no;
                        is_full = false;
                    }
                }
                if is_full {
                    return self.info.clone();
                }
                self.user_addr.insert(user_no, addr);
                self.set_head();
                self.spread_info();
                let _ = get_into_room(&GetInRoomForm { room_id: self.info.id }, self.pool.clone());
            }
            RoomJoin::Observe(addr) => {
                self.observe.insert(addr);
                self.info.observer_cnt += 1;
                self.spread_info();
            }
            RoomJoin::List(addr) => {
                self.list.insert(addr);
            }
        }
        self.info.clone()
    }
}

/// Leaves room
/// If the information is invalid or not present, it will do nothing.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub enum RoomLeave {
    User(UserNo),
    Observe(Addr<Session<Observe>>),
    List(Addr<Session<List>>),
}

impl Handler<RoomLeave> for Room {
    type Result = ();

    fn handle(&mut self, msg: RoomLeave, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            RoomLeave::User(user_no) => {
                if self.info.is_game {
                    return;
                }
                if self.user_addr.remove(&user_no).is_none() {
                    return;
                }

                for i in self.info.user.iter_mut() {
                    if *i == user_no {
                        i.0 = 0;
                    }
                }
                self.set_head();
                self.spread_info();

                if self.user_addr.is_empty() {
                    self.hub.do_send(RemoveRoom(self.info.id));
                    ctx.stop();
                }
                let _ = leave_room(&LeaveRoomForm { room_id: self.info.id }, self.pool.clone());
            }
            RoomLeave::Observe(addr) => {
                if !self.observe.remove(&addr) {
                    return;
                }
                self.info.observer_cnt -= 1;
                self.spread_info();
            }
            RoomLeave::List(addr) => {
                self.list.remove(&addr);
            }
        }
    }
}

/// Changes the name of the room.
/// It won't be changed if the user is not head.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct ChangeName(pub UserNo, pub String);

impl Handler<ChangeName> for Room {
    type Result = ();

    fn handle(&mut self, msg: ChangeName, _: &mut Self::Context) -> Self::Result {
        if msg.0 != self.info.head {
            return;
        }
        self.info.name = msg.1;
        self.spread_info();
        let form = ChangeRoomInfoForm {
            room_id: self.info.id,
            name: Some(self.info.name.clone()),
            rule: None,
        };
        let _ = change_room_info(&form, self.pool.clone());
    }
}

/// Changes the rule of the room.
/// It won't be changed if the user is not head.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct ChangeRule(pub UserNo, pub Rule);

impl Handler<ChangeRule> for Room {
    type Result = ();

    fn handle(&mut self, msg: ChangeRule, _: &mut Self::Context) -> Self::Result {
        if msg.0 != self.info.head || self.info.is_game {
            return;
        }
        self.info.rule = RuleHash::generate(&msg.1);
        let _ = save_rule(&SaveRuleForm { rule: msg.1.clone() }, self.pool.clone());

        self.spread_info();
        let form = ChangeRoomInfoForm {
            room_id: self.info.id,
            name: None,
            rule: Some(msg.1),
        };
        let _ = change_room_info(&form, self.pool.clone());
    }
}

/// Starts the game.
/// It won't be changed if the user is not head.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct StartGame(pub UserNo);

impl Handler<StartGame> for Room {
    type Result = ();

    fn handle(&mut self, msg: StartGame, _: &mut Self::Context) -> Self::Result {
        if msg.0 != self.info.head || self.info.is_game {
            return;
        }
        let id = GameId::generate_random();
        let rule = get_rule(
            &GetRuleForm {
                rule_hash: self.info.rule,
            },
            self.pool.clone(),
        )
        .unwrap();
        self.game = Some(GameInfo {
            id,
            no: 0,
            game: Game::new(rule.clone()),
        });
        self.info.is_game = true;
        self.spread_info();
        self.spread_game();
        let form = MakeGameForm {
            game_id: id,
            room_id: self.info.uid,
            room_name: self.info.name.clone(),
            users: self.info.user.iter().map(|x| x.0).collect(),
            is_rank: true,
            rule,
        };
        let _ = make_game(&form, self.pool.clone());
    }
}

/// Process the game
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct Go(pub UserNo, pub Command);

impl Handler<Go> for Room {
    type Result = ();

    fn handle(&mut self, msg: Go, _: &mut Self::Context) -> Self::Result {
        if !self.info.is_game {
            return;
        }

        let mut user_id = self.info.user.len();
        for (i, x) in self.info.user.iter().enumerate() {
            if *x == msg.0 {
                user_id = i;
                break;
            }
        }

        if user_id == self.info.user.len() {
            return;
        }
        let finished = ignore!(self.next(user_id, msg.1));
        let game = self.game.as_ref().unwrap();
        let _ = save_state(
            &SaveStateForm {
                game_id: game.id,
                room_id: self.info.uid,
                number: game.no,
                state: game.game.get_state(),
            },
            self.pool.clone(),
        );

        if finished {
            if self.info.is_rank {
                if let Some(game) = &mut self.game {
                    if let State::GameEnded {
                        winner,
                        president,
                        score,
                        ..
                    } = game.game.state
                    {
                        for (i, &userno) in self.info.user.iter().enumerate() {
                            let score = if (1 << i) & winner > 0 {
                                score as i32
                            } else {
                                -(score as i32)
                            };
                            let score = if i == president { 2 * score } else { score };
                            self.user_addr
                                .get(&userno)
                                .unwrap()
                                .do_send(ChangeRating(score, game.id));
                        }
                    }
                }
            }
            self.info.is_game = false;
            self.game = None;
            self.spread_info();
        }
    }
}

/// Returns the information of this room.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub enum Chat {
    User(String, UserNo),
    Observe(String, UserNo),
}

impl Handler<Chat> for Room {
    type Result = ();

    fn handle(&mut self, msg: Chat, _: &mut Self::Context) -> Self::Result {
        match msg {
            Chat::User(chat, no) => {
                for (_, i) in self.user_addr.iter() {
                    i.do_send(SendChat(chat.clone(), no));
                }

                for i in self.observe.iter() {
                    i.do_send(ObserveToClient::Chat(chat.clone(), no));
                }
            }
            Chat::Observe(chat, no) => {
                for i in self.observe.iter() {
                    i.do_send(ObserveToClient::Chat(chat.clone(), no));
                }
            }
        }
    }
}

/// Returns the information of this room.
#[derive(Debug, Clone, Message)]
#[rtype(result = "RoomInfo")]
pub struct GetInfo;

impl Handler<GetInfo> for Room {
    type Result = RoomInfo;

    fn handle(&mut self, _: GetInfo, _: &mut Self::Context) -> Self::Result {
        self.info.clone()
    }
}

impl Room {
    pub fn new(info: RoomInfo, server: Addr<hub::Hub>, pool: Pool) -> Room {
        Room {
            info,
            game: None,
            user_addr: HashMap::new(),
            observe: HashSet::new(),
            list: HashSet::new(),
            hub: server,
            pool,
        }
    }

    fn set_head(&mut self) {
        if !self.user_addr.contains_key(&self.info.head) {
            self.info.head.0 = 0;
        }

        if self.info.head.0 == 0 {
            for i in self.info.user.iter() {
                if i.0 != 0 {
                    self.info.head.0 = 0;
                }
            }
        }
    }

    fn next(&mut self, user_id: usize, cmd: Command) -> Result<bool> {
        if let Some(game) = &mut self.game {
            let res = game.game.next(user_id, cmd)?;
            self.spread_game();
            Ok(res)
        } else {
            bail!("game not started")
        }
    }

    fn spread_info(&self) {
        for (_, i) in self.user_addr.iter() {
            i.do_send(GotRoomInfo(self.info.clone()));
        }

        for i in self.observe.iter() {
            i.do_send(ObserveToClient::Room(self.info.clone()));
        }

        let simple_info = SimpleRoomInfo::from(self.info.clone());

        for i in self.list.iter() {
            i.do_send(ListToClient::Room(simple_info.clone()));
        }
    }

    // assert: game is not `None`
    fn spread_game(&self) {
        let state = self.game.as_ref().unwrap().game.get_state();
        for (_, i) in self.user_addr.iter() {
            i.do_send(GotGameState(state.clone()));
        }

        for i in self.observe.iter() {
            i.do_send(ObserveToClient::Game(state.clone()));
        }
    }

    /*fn calculate_rating(&self, score: i32) -> i32 {

    }*/
}
