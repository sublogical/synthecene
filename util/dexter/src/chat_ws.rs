use warp::ws::{Message, WebSocket};
use futures::{FutureExt, StreamExt};
use tokio_stream::wrappers::UnboundedReceiverStream;




async fn chat_ws_handler(ws: warp::ws::Ws, chat_id: String) -> Result<impl warp::Reply, warp::Rejection> {
    if chat_id == "foo" {
        Ok(ws.on_upgrade(|websocket| chat_ws(websocket, chat_id)))
    } else {
        Err(warp::reject::not_found())
    }
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
enum ChatChannelMessage {
    Message(ChatMessage),
    End
}

struct ChatChannelClient {
    channel_id: String,
    receiver: mpsc::UnboundedReceiver<ChatChannelMessage>,
}

async fn chat_ws(websocket: warp::ws::WebSocket, chat_id: String) {
    let (mut ws_tx, mut ws_rx) = websocket.split();
    let (client_sender, client_rcv) = mpsc::unbounded_channel();
    let client_rcv = UnboundedReceiverStream::new(client_rcv);

    let client = ChatChannelClient {
        channel_id: chat_id,
        receiver: client_rcv,
    };

    
    todo!()
}


