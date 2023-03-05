mod utils;
mod store;
mod cmd;

use std::sync::{Arc, Mutex};
use anyhow::Result;
use tokio::net::TcpListener;
use utils::handle_connection;
use crate::store::Store;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let main_store = Arc::new(Mutex::new(Store::new()));

    loop {
        let incoming = listener.accept().await;
        let client_store = main_store.clone();

        match incoming {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_connection(stream, client_store).await.unwrap();
                });
            }
            Err(e) => {
                eprintln!("error: {e}");
            }
        }
    }
}
