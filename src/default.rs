use crate::client::Client;
use crate::client::UringCfg;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref DEFAULT_CLIENT: Client =
        Client::build(UringCfg::default()).expect("failed to build default client");
}

pub fn default_client() -> &'static Client {
    &DEFAULT_CLIENT
}
