use crate::prelude::*;
use crate::ws::session::{Context, Session, SessionTrait};
use types::{ObserveToClient, ObserveToServer};

pub struct ObserveSession;

impl SessionTrait for ObserveSession {
    type Sender = ObserveToServer;

    fn tag() -> &'static str {
        "observe"
    }

    fn receive(&mut self, msg: String, _: &Context<Self>) -> (&str, JsValue) {
        let msg: ObserveToClient = serde_json::from_str(&*msg).unwrap();
        match msg {
            ObserveToClient::Room(info) => ("room_info", JsValue::from_serde(&info).unwrap()),
            ObserveToClient::Game(state) => ("game_state", JsValue::from_serde(&state).unwrap()),
            ObserveToClient::Chat(chat, no) => ("chat", JsValue::from_serde(&(chat, no)).unwrap()),
        }
    }
}

#[wasm_bindgen]
pub struct Observe {
    session: Session<ObserveSession>,
}

#[wasm_bindgen]
impl Observe {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<Observe> {
        Ok(Observe {
            session: ObserveSession.start()?,
        })
    }

    pub fn on(&self, tag: String, callback: Function) {
        self.session.on(tag, callback);
    }
}
