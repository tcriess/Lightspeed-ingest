use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::ops::DerefMut;
use std::sync::Arc;

use dns_lookup::lookup_host;
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use hex::{decode, encode};
use log::{error, info, warn};
use rand::distributions::{Alphanumeric, Uniform};
use rand::{thread_rng, Rng};
use ring::hmac;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_util::codec::Framed;

use crate::ftl_codec::{FtlCodec, FtlCommand};
use std::cmp::Ordering;

#[derive(Debug)]
enum FrameCommand {
    Send { data: Vec<String> },
    // Kill,
}

#[derive(Debug)]
enum FwCommand {
    Send { data: [u8; 4096], len: usize },
    LookupResult { addrs: Vec<(String, String)> },
}

pub struct Connection {}

#[derive(Debug)]
pub struct ConnectionState {
    pub hmac_payload: Option<String>,
    pub channel: Option<String>,
    pub protocol_version: Option<String>,
    pub vendor_name: Option<String>,
    pub vendor_version: Option<String>,
    pub video: bool,
    pub video_codec: Option<String>,
    pub video_height: Option<String>,
    pub video_width: Option<String>,
    pub video_payload_type: Option<String>,
    pub video_ingest_ssrc: Option<String>,
    pub audio: bool,
    pub audio_codec: Option<String>,
    pub audio_payload_type: Option<String>,
    pub audio_ingest_ssrc: Option<String>,
}

impl ConnectionState {
    pub fn get_payload(&self) -> String {
        match &self.hmac_payload {
            Some(payload) => payload.clone(),
            None => String::new(),
        }
    }
    pub fn get_channel(&self) -> String {
        match &self.channel {
            Some(s) => s.clone(),
            None => String::new(),
        }
    }
    pub fn new() -> ConnectionState {
        ConnectionState {
            hmac_payload: None,
            channel: None,
            protocol_version: None,
            vendor_name: None,
            vendor_version: None,
            video: false,
            video_codec: None,
            video_height: None,
            video_width: None,
            video_payload_type: None,
            video_ingest_ssrc: None,
            audio: false,
            audio_codec: None,
            audio_ingest_ssrc: None,
            audio_payload_type: None,
        }
    }
    pub fn print(&self) {
        match &self.protocol_version {
            Some(p) => info!("Protocol Version: {}", p),
            None => warn!("Protocol Version: None"),
        }
        match &self.vendor_name {
            Some(v) => info!("Vendor Name: {}", v),
            None => warn!("Vendor Name: None"),
        }
        match &self.vendor_version {
            Some(v) => info!("Vendor Version: {}", v),
            None => warn!("Vendor Version: None"),
        }
        match &self.video_codec {
            Some(v) => info!("Video Codec: {}", v),
            None => warn!("Video Codec: None"),
        }

        match &self.video_height {
            Some(v) => info!("Video Height: {}", v),
            None => warn!("Video Height: None"),
        }
        match &self.video_width {
            Some(v) => info!("Video Width: {}", v),
            None => warn!("Video Width: None"),
        }
        match &self.audio_codec {
            Some(a) => info!("Audio Codec: {}", a),
            None => warn!("Audio Codec: None"),
        }
    }
}

impl Connection {
    //initialize connection
    pub fn init(stream: TcpStream, socket: UdpSocket, forward_suffix: &str) {
        //Initialize 2 channels so we can communicate between the frame task and the command handling task
        let (frame_send, mut conn_receive) = mpsc::channel::<FtlCommand>(2);
        let (conn_send, mut frame_receive) = mpsc::channel::<FrameCommand>(2);
        let (rtp_send, mut rtp_recv) = mpsc::channel::<(SocketAddr, String)>(2);

        let peer_addr = stream.peer_addr().unwrap();
        //spawn a task whos sole job is to interact with the frame to send and receive information through the codec
        tokio::spawn(async move {
            let mut frame = Framed::new(stream, FtlCodec::new());
            loop {
                //wait until there is a command present
                match frame.next().await {
                    Some(Ok(command)) => {
                        //send the command to the command handling task
                        match frame_send.send(command).await {
                            Ok(_) => {
                                //wait for the command handling task to send us instructions
                                let command = frame_receive.recv().await;
                                //handle the instructions that we received
                                match handle_frame_command(command, &mut frame).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("There was an error handing frame command {:?}", e);
                                        return;
                                    }
                                };
                            }
                            Err(e) => {
                                error!(
                                    "There was an error sending the command to the connection Error: {:?}", e
                                );
                                return;
                            }
                        };
                    }
                    Some(Err(e)) => {
                        error!("There was an error {:?}", e);
                        return;
                    }
                    None => {
                        error!("There was a socket reading error");
                        return;
                    }
                };
            }
        });

        tokio::spawn(async move {
            //initialize new connection state
            let mut state = ConnectionState::new();
            loop {
                //wait until the frame task sends us a command
                match conn_receive.recv().await {
                    Some(FtlCommand::Disconnect) => {
                        //TODO: Determine what needs to happen here
                    }
                    //this command is where we tell the client what port to use
                    //WARNING: This command does not work properly.
                    //For some reason the client does not like the port we are sending and defaults to 65535 this is fine for now but will be fixed in the future
                    Some(FtlCommand::Dot) => {
                        let resp_string = "200 hi. Use UDP port 65535\n".to_string();
                        let mut resp = Vec::new();
                        resp.push(resp_string);
                        //tell the frame task to send our response
                        match conn_send.send(FrameCommand::Send { data: resp }).await {
                            Ok(_) => {
                                info!("Client connected!");

                                let channel = state.get_channel();

                                match rtp_send.send((peer_addr, channel)).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Error intializing rtp forward {:?}", e);
                                        return;
                                    }
                                }
                                state.print()
                            }
                            Err(e) => {
                                error!("Error sending to frame task (From: Handle HMAC) {:?}", e);
                                return;
                            }
                        }
                    }
                    Some(command) => {
                        handle_command(command, &conn_send, &mut state).await;
                    }
                    None => {
                        error!("Nothing received from the frame");
                        return;
                    }
                }
            }
        });

        if !forward_suffix.is_empty() {
            let (lookup_send, mut lookup_recv) = mpsc::channel::<String>(2);
            let (fw_send, mut fw_recv) = mpsc::channel::<FwCommand>(4096);
            let fw_send_from_lookup = fw_send.clone();
            tokio::spawn(async move {
                loop {
                    match lookup_recv.recv().await {
                        Some(hostname) => {
                            let mut ips: Vec<IpAddr> = vec![];
                            loop {
                                let sleeper = sleep(Duration::from_millis(60000));
                                let mut new_ips = vec![];
                                match lookup_host(&hostname) {
                                    Ok(res) => {
                                        for addr in res {
                                            new_ips.push(addr);
                                        }
                                    }
                                    Err(e) => {
                                        error!("could not lookup host {}: {}", hostname, e);
                                    }
                                }
                                new_ips.sort();
                                let mut diff = false;
                                if new_ips.len() != ips.len() {
                                    diff = true;
                                } else {
                                    for (i, ip) in ips.iter().enumerate() {
                                        if ip.cmp(&new_ips[i]) != Ordering::Equal {
                                            diff = true;
                                            break;
                                        }
                                    }
                                }
                                if diff {
                                    ips = new_ips;
                                    let mut addr_strings = vec![];
                                    for ip in &ips {
                                        if ip.is_ipv6() {
                                            addr_strings.push((
                                                "[::]:0".to_string(),
                                                format!("[{}]", ip.to_string()),
                                            ));
                                        } else {
                                            addr_strings
                                                .push(("0.0.0.0:0".to_string(), ip.to_string()));
                                        }
                                    }
                                    fw_send_from_lookup
                                        .send(FwCommand::LookupResult {
                                            addrs: addr_strings,
                                        })
                                        .await
                                        .unwrap();
                                }
                                sleeper.await;
                            }
                        }
                        None => {
                            return;
                        }
                    }
                }
            });
            tokio::spawn(async move {
                // forward task, receive data to send via the fw_ channel
                // the first thing that should be sent here is a LookupResult command which contains the list of addresses to forward to
                let fw_sockets: Arc<Mutex<Vec<UdpSocket>>> = Arc::new(Mutex::new(vec![]));
                loop {
                    match fw_recv.recv().await {
                        Some(fw_command) => match fw_command {
                            FwCommand::Send { data, len } => {
                                let mut fw_s = fw_sockets.lock().await;
                                let mut futs = Vec::new();
                                for fw_socket in fw_s.deref_mut() {
                                    futs.push(fw_socket.send(&data[0..len]));
                                }
                                join_all(futs).await;
                            }
                            FwCommand::LookupResult { addrs } => {
                                fw_sockets.lock().await.deref_mut().clear();
                                for (listen_addr, addr_str) in addrs {
                                    let s = UdpSocket::bind(listen_addr).await.unwrap();
                                    match s.connect(format!("{}:65535", addr_str)).await {
                                        Ok(_) => {
                                            fw_sockets.lock().await.deref_mut().push(s);
                                        }
                                        Err(e) => {
                                            error!("error connecting to remote: {:?}", e)
                                        }
                                    }
                                }
                            }
                        },
                        None => {
                            error!("could not read from fw channel");
                            return;
                        }
                    }
                }
            });
            let fw_suffix = format!("{}", forward_suffix);
            tokio::spawn(async move {
                loop {
                    match rtp_recv.recv().await {
                        Some((addr, channel)) => {
                            let hostname = format!("ch-{}.{}", channel.to_owned(), fw_suffix);
                            info!("rtp_rcv received, connecting udp");
                            let new_addr = SocketAddr::new(addr.ip(), 0);
                            socket
                                .connect(new_addr)
                                .await
                                .expect("Could not connect udp socket");
                            let mut buf = [0; 4096];
                            let mut to_send = 0;
                            lookup_send.send(hostname.to_string()).await.unwrap();
                            loop {
                                if to_send > 0 {
                                    fw_send
                                        .send(FwCommand::Send {
                                            data: buf,
                                            len: to_send,
                                        })
                                        .await
                                        .unwrap();
                                }
                                to_send = socket.recv(&mut buf).await.unwrap();
                            }
                        }
                        None => {
                            error!("could not receive udp");
                            return;
                        }
                    }
                }
            });
        }
    }
}

async fn handle_frame_command(
    command: Option<FrameCommand>,
    frame: &mut Framed<TcpStream, FtlCodec>,
) -> Result<(), String> {
    match command {
        Some(FrameCommand::Send { data }) => {
            let mut d: Vec<String> = data.clone();
            d.reverse();
            while !d.is_empty() {
                let item = d.pop().unwrap();
                match frame.send(item.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        info!("There was an error {:?}", e);
                        return Err(format!("There was an error {:?}", e));
                    }
                }
            }

            return Ok(());
        }
        // Some(FrameCommand::Kill) => {
        //     info!("TODO: Implement Kill command");
        //     return Ok(());
        // }
        None => {
            info!("Error receiving command from conn");
            return Err("Error receiving command from conn".to_string());
        }
    };
}

async fn handle_command(
    command: FtlCommand,
    sender: &mpsc::Sender<FrameCommand>,
    conn: &mut ConnectionState,
) {
    match command {
        FtlCommand::HMAC => {
            conn.hmac_payload = Some(generate_hmac());
            let resp = vec!["200 ".to_string(), conn.get_payload(), "\n".to_string()];
            match sender.send(FrameCommand::Send { data: resp }).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    error!("Error sending to frame task (From: Handle HMAC) {:?}", e);
                    return;
                }
            }
        }
        FtlCommand::Connect { data } => {
            //make sure we receive a valid channel id and stream key
            match (data.get("stream_key"), data.get("channel_id")) {
                (Some(key), Some(channel_id)) => {
                    //decode the client hash
                    let client_hash = hex::decode(key).expect("error with hash decode");
                    let key = hmac::Key::new(hmac::HMAC_SHA512, &read_stream_key(false, Some("")));
                    //compare the two hashes to ensure they match
                    match hmac::verify(
                        &key,
                        decode(conn.get_payload().into_bytes())
                            .expect("error with payload decode")
                            .as_slice(),
                        &client_hash.as_slice(),
                    ) {
                        Ok(_) => {
                            info!("Hashes match!");
                            conn.channel = Some(channel_id.to_string());
                            let resp = vec!["200\n".to_string()];
                            match sender.send(FrameCommand::Send { data: resp }).await {
                                Ok(_) => {
                                    return;
                                }
                                Err(e) => error!(
                                    "Error sending to frame task (From: Handle Connection) {:?}",
                                    e
                                ),
                            }
                        }
                        _ => {
                            error!("Hashes do not equal");
                            return;
                        }
                    };
                }

                (None, _) => {
                    error!("No stream key attached to connect command");
                    return;
                }
                (_, None) => {
                    error!("No channel id attached to connect command");
                    return;
                }
            }
        }
        FtlCommand::Attribute { data } => {
            match (data.get("key"), data.get("value")) {
                (Some(key), Some(value)) => {
                    // info!("Key: {:?}, value: {:?}", key, value);
                    match key.as_str() {
                        "ProtocolVersion" => conn.protocol_version = Some(value.to_string()),
                        "VendorName" => conn.vendor_name = Some(value.to_string()),
                        "VendorVersion" => conn.vendor_version = Some(value.to_string()),
                        "Video" => {
                            match value.as_str() {
                                "true" => conn.video = true,
                                "false" => conn.video = false,
                                _ => {
                                    error!("Invalid video value! Atrribute parse failed. Value was: {:?}", value);
                                    return;
                                }
                            }
                        }
                        "VideoCodec" => conn.video_codec = Some(value.to_string()),
                        "VideoHeight" => conn.video_height = Some(value.to_string()),
                        "VideoWidth" => conn.video_width = Some(value.to_string()),
                        "VideoPayloadType" => conn.video_payload_type = Some(value.to_string()),
                        "VideoIngestSSRC" => conn.video_ingest_ssrc = Some(value.to_string()),
                        "Audio" => {
                            match value.as_str() {
                                "true" => conn.audio = true,
                                "false" => conn.audio = false,
                                _ => {
                                    error!("Invalid audio value! Atrribute parse failed. Value was: {:?}", value);
                                    return;
                                }
                            }
                        }
                        "AudioCodec" => conn.audio_codec = Some(value.to_string()),
                        "AudioPayloadType" => conn.audio_payload_type = Some(value.to_string()),
                        "AudioIngestSSRC" => conn.audio_ingest_ssrc = Some(value.to_string()),
                        _ => {
                            error!("Invalid attribute command. Attribute parsing failed. Key was {:?}, Value was {:?}", key, value)
                        }
                    }
                    // No actual response is expected but if we do not respond at all the client
                    // stops sending for some reason.
                    let resp = vec!["".to_string()];
                    match sender.send(FrameCommand::Send { data: resp }).await {
                        Ok(_) => {
                            return;
                        }
                        Err(e) => error!(
                            "Error sending to frame task (From: Handle Connection) {:?}",
                            e
                        ),
                    }
                }
                (None, Some(_value)) => {}
                (Some(_key), None) => {}
                (None, None) => {}
            }
        }
        FtlCommand::Ping => {
            // info!("Handling PING Command");
            let resp = vec!["201\n".to_string()];
            match sender.send(FrameCommand::Send { data: resp }).await {
                Ok(_) => {
                    return;
                }
                Err(e) => error!(
                    "Error sending to frame task (From: Handle Connection) {:?}",
                    e
                ),
            }
        }
        _ => {
            warn!("Command not implemented yet. Tell GRVY to quit his day job");
            return;
        }
    }
}

fn generate_hmac() -> String {
    let dist = Uniform::new(0x00, 0xFF);
    let mut hmac_payload: Vec<u8> = Vec::new();
    let mut rng = thread_rng();
    for _ in 0..128 {
        hmac_payload.push(rng.sample(dist));
    }
    encode(hmac_payload.as_slice())
}

fn generate_stream_key() -> Vec<u8> {
    let stream_key: String = String::from_utf8(thread_rng()
        .sample_iter(&Alphanumeric).take(32).collect())
        .expect("Failed to convert random key to string! Please open an issue and tell the devs to handle this!");
    fs::write("hash", hex::encode(&stream_key)).expect("Unable to write file");

    stream_key.as_bytes().to_vec()
}

fn print_stream_key(stream_key: Vec<u8>) {
    info!(
        // ANSI escape codes to color stream key output
        "Your stream key is: \x1b[31;1;4m77-{}\x1b[0m",
        std::str::from_utf8(&stream_key).unwrap()
    );
}

pub fn read_stream_key(startup: bool, stream_key_env: Option<&str>) -> Vec<u8> {
    if startup {
        if let Some(stream_key) = stream_key_env {
            if !stream_key.is_empty() {
                let key = stream_key.as_bytes().to_vec();
                print_stream_key(key.to_vec());
                fs::write("hash", hex::encode(&stream_key))
                    .expect("Unable to write stream key to hash file");
                return key;
            }
        }
        match fs::read_to_string("hash") {
            Err(_) => {
                let stream_key = generate_stream_key();
                warn!("Could not read stream key. Re-generating...");
                print_stream_key(stream_key.to_vec());
                stream_key
            }
            Ok(file) => {
                info!("Loading existing stream key...");
                match hex::decode(file) {
                    Err(_) => {
                        let stream_key = generate_stream_key();
                        warn!("Error decoding stream key. Re-generating...");
                        print_stream_key(stream_key.to_vec());
                        stream_key
                    }
                    Ok(stream_key) => {
                        print_stream_key(stream_key.to_vec());
                        stream_key
                    }
                }
            }
        }
    } else {
        let file = fs::read_to_string("hash").unwrap();
        hex::decode(file).unwrap()
    }
}
