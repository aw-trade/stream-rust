use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use tokio;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketData {
    symbol: String,
    price: f64,
    volume: f64,
    bid: f64,
    ask: f64,
    timestamp: u64,
    exchange: String,
}

#[derive(Debug, Deserialize)]
struct StreamRequest {
    action: String, // "start" or "stop"
    symbol: String, // e.g., "BTC", "ETH"
}

#[derive(Debug, Clone)]
struct Client {
    addr: SocketAddr,
    subscribed_symbols: Vec<String>,
}

// Coinbase WebSocket message formats
#[derive(Debug, Serialize)]
struct CoinbaseSubscribeMessage {
    #[serde(rename = "type")]
    message_type: String,
    product_ids: Vec<String>,
    channels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseTickerMessage {
    #[serde(rename = "type")]
    message_type: String,
    sequence: Option<u64>,
    product_id: String,
    price: String,
    open_24h: Option<String>,
    volume_24h: Option<String>,
    low_24h: Option<String>,
    high_24h: Option<String>,
    volume_30d: Option<String>,
    best_bid: String,
    best_ask: String,
    side: Option<String>,
    time: String,
    trade_id: Option<u64>,
    last_size: Option<String>,
}

type ClientMap = Arc<Mutex<HashMap<SocketAddr, Client>>>;

struct CryptoStreamer {
    socket: UdpSocket,
    clients: ClientMap,
}

impl CryptoStreamer {
    fn new(bind_addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        
        Ok(CryptoStreamer {
            socket,
            clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn symbol_to_coinbase(&self, symbol: &str) -> String {
        match symbol.to_uppercase().as_str() {
            "BTC" => "BTC-USD".to_string(),
            "ETH" => "ETH-USD".to_string(),
            "ADA" => "ADA-USD".to_string(),
            "SOL" => "SOL-USD".to_string(),
            "DOT" => "DOT-USD".to_string(),
            "LINK" => "LINK-USD".to_string(),
            "UNI" => "UNI-USD".to_string(),
            "LTC" => "LTC-USD".to_string(),
            "XRP" => "XRP-USD".to_string(),
            "DOGE" => "DOGE-USD".to_string(),
            "MATIC" => "MATIC-USD".to_string(),
            "AVAX" => "AVAX-USD".to_string(),
            "ATOM" => "ATOM-USD".to_string(),
            "NEAR" => "NEAR-USD".to_string(),
            "ALGO" => "ALGO-USD".to_string(),
            _ => format!("{}-USD", symbol.to_uppercase()),
        }
    }

    fn coinbase_to_symbol(&self, coinbase_symbol: &str) -> String {
        let upper_symbol = coinbase_symbol.to_uppercase();
        match upper_symbol.as_str() {
            "BTC-USD" => "BTC".to_string(),
            "ETH-USD" => "ETH".to_string(),
            "ADA-USD" => "ADA".to_string(),
            "SOL-USD" => "SOL".to_string(),
            "DOT-USD" => "DOT".to_string(),
            "LINK-USD" => "LINK".to_string(),
            "UNI-USD" => "UNI".to_string(),
            "LTC-USD" => "LTC".to_string(),
            "XRP-USD" => "XRP".to_string(),
            "DOGE-USD" => "DOGE".to_string(),
            "MATIC-USD" => "MATIC".to_string(),
            "AVAX-USD" => "AVAX".to_string(),
            "ATOM-USD" => "ATOM".to_string(),
            "NEAR-USD" => "NEAR".to_string(),
            "ALGO-USD" => "ALGO".to_string(),
            _ => {
                // Remove -USD suffix if present
                if upper_symbol.ends_with("-USD") {
                    upper_symbol.strip_suffix("-USD").unwrap_or(&upper_symbol).to_string()
                } else {
                    upper_symbol
                }
            }
        }
    }

    async fn connect_to_coinbase_stream(&self) -> Result<(), Box<dyn std::error::Error>> {
        let url = "wss://ws-feed.exchange.coinbase.com";
        
        println!("Connecting to Coinbase WebSocket: {}", url);

        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        println!("✓ Connected to Coinbase real-time stream");

        // Subscribe to ticker channel for major cryptocurrencies
        let product_ids = vec![
            "BTC-USD", "ETH-USD", "ADA-USD", "SOL-USD", "DOT-USD",
            "LINK-USD", "UNI-USD", "LTC-USD", "XRP-USD", "DOGE-USD",
            "MATIC-USD", "AVAX-USD", "ATOM-USD", "NEAR-USD", "ALGO-USD"
        ];

        let subscribe_msg = CoinbaseSubscribeMessage {
            message_type: "subscribe".to_string(),
            product_ids: product_ids.iter().map(|s| s.to_string()).collect(),
            channels: vec!["ticker".to_string()],
        };

        let subscribe_json = serde_json::to_string(&subscribe_msg)?;
        // Fix: Convert String to bytes and use Message::Text or Message::Binary
        write.send(Message::Text(subscribe_json.into())).await?;

        println!("✓ Subscribed to {} products", product_ids.len());

        // Process incoming messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // Convert bytes to string if needed
                    let text_str = text.to_string();
                    
                    // Try to parse as ticker message
                    if let Ok(ticker_msg) = serde_json::from_str::<CoinbaseTickerMessage>(&text_str) {
                        if ticker_msg.message_type == "ticker" {
                            let market_data = self.parse_coinbase_message(ticker_msg);
                            self.broadcast_to_clients(&market_data).await;
                        }
                    } else {
                        // Handle other message types (subscriptions, heartbeat, etc.)
                        if text_str.contains("\"type\":\"subscriptions\"") {
                            println!("✓ Subscription confirmed");
                        } else if text_str.contains("\"type\":\"heartbeat\"") {
                            // Heartbeat - keep connection alive
                        } else {
                            println!("📋 Other message: {}", text_str);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    println!("WebSocket connection closed");
                    break;
                }
                Err(e) => {
                    eprintln!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn parse_coinbase_message(&self, msg: CoinbaseTickerMessage) -> MarketData {
        let symbol = self.coinbase_to_symbol(&msg.product_id);
        
        MarketData {
            symbol,
            price: msg.price.parse().unwrap_or(0.0),
            volume: msg.volume_24h.unwrap_or_default().parse().unwrap_or(0.0),
            bid: msg.best_bid.parse().unwrap_or(0.0),
            ask: msg.best_ask.parse().unwrap_or(0.0),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            exchange: "Coinbase".to_string(),
        }
    }

    async fn broadcast_to_clients(&self, market_data: &MarketData) {
        let clients = self.clients.lock().unwrap().clone();
        
        if clients.is_empty() {
            return;
        }

        let json_data = serde_json::to_string(market_data).unwrap();
        let mut clients_to_send = 0;

        for client in clients.values() {
            if client.subscribed_symbols.contains(&market_data.symbol) {
                if let Err(e) = self.socket.send_to(json_data.as_bytes(), client.addr) {
                    eprintln!("Failed to send data to {}: {}", client.addr, e);
                } else {
                    clients_to_send += 1;
                }
            }
        }

        if clients_to_send > 0 {
            println!("📡 {} ${:.4} → {} clients", 
                    market_data.symbol, market_data.price, clients_to_send);
        }
    }

    fn handle_client_requests(&self) {
        let mut buf = [0; 1024];
        
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((size, addr)) => {
                    let msg = String::from_utf8_lossy(&buf[..size]);
                    
                    if let Ok(request) = serde_json::from_str::<StreamRequest>(&msg) {
                        self.process_request(request, addr);
                    } else {
                        println!("Invalid request from {}: {}", addr, msg);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    eprintln!("Error receiving UDP data: {}", e);
                }
            }
        }
    }

    fn process_request(&self, request: StreamRequest, addr: SocketAddr) {
        let mut clients = self.clients.lock().unwrap();
        
        match request.action.as_str() {
            "start" => {
                let client = clients.entry(addr).or_insert_with(|| Client {
                    addr,
                    subscribed_symbols: Vec::new(),
                });
                
                let symbol = request.symbol.to_uppercase();
                
                if !client.subscribed_symbols.contains(&symbol) {
                    client.subscribed_symbols.push(symbol.clone());
                    println!("✓ Client {} subscribed to {}", addr, symbol);
                } else {
                    println!("ℹ️  Client {} already subscribed to {}", addr, symbol);
                }
            }
            "stop" => {
                if let Some(client) = clients.get_mut(&addr) {
                    let symbol = request.symbol.to_uppercase();
                    client.subscribed_symbols.retain(|s| s != &symbol);
                    println!("✓ Client {} unsubscribed from {}", addr, symbol);
                    
                    if client.subscribed_symbols.is_empty() {
                        clients.remove(&addr);
                        println!("✓ Client {} removed (no active subscriptions)", addr);
                    }
                }
            }
            _ => {
                println!("Unknown action: {} from {}", request.action, addr);
            }
        }
    }

    pub async fn run(&self) {
        println!("🚀 Real-time Crypto Streamer started on {}", 
                self.socket.local_addr().unwrap());
        println!("📊 Streaming live data from Coinbase WebSocket");
        
        // Spawn thread for handling client requests
        let socket_clone = self.socket.try_clone().unwrap();
        let clients_clone = Arc::clone(&self.clients);
        
        thread::spawn(move || {
            let streamer = CryptoStreamer {
                socket: socket_clone,
                clients: clients_clone,
            };
            streamer.handle_client_requests();
        });

        // Connect to Coinbase stream (this will run indefinitely)
        loop {
            match self.connect_to_coinbase_stream().await {
                Ok(_) => {
                    println!("Coinbase stream ended, reconnecting...");
                }
                Err(e) => {
                    eprintln!("Failed to connect to Coinbase: {}", e);
                    println!("Retrying in 5 seconds...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // IMPORTANT: Bind to 0.0.0.0 instead of 127.0.0.1 for Docker container
    // This allows connections from outside the container
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8888".to_string());
    println!("Starting server on: {}", bind_addr);
    
    let streamer = CryptoStreamer::new(&bind_addr)?;
    streamer.run().await;
    Ok(())
}