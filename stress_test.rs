use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MASTER: &str = "127.0.0.1:6389";
const REPLICAS: &[&str] = &[
    "127.0.0.1:6390",
    "127.0.0.1:6391",
    "127.0.0.1:6392",
    "127.0.0.1:6393",
];

const WRITE_CLIENTS: usize = 50;
const READ_CLIENTS_PER_REPLICA: usize = 40;
const TEST_DURATION_SECS: u64 = 15;

fn encode_set(key: &str, value: &str) -> Vec<u8> {
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(), key, value.len(), value
    ).into_bytes()
}

fn encode_get(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

fn encode_incr(key: &str) -> Vec<u8> {
    format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

fn writer_thread(
    id: usize,
    running: Arc<AtomicBool>,
    write_count: Arc<AtomicU64>,
    write_errors: Arc<AtomicU64>,
) {
    let mut stream = match TcpStream::connect(MASTER) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Writer {id} connect failed: {e}");
            return;
        }
    };
    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let mut buf = [0u8; 1024];
    let mut counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let key = format!("k:{id}:{}", counter % 50);
        let value = format!("v:{counter}:{}", "x".repeat(100));
        
        let cmd = if counter % 5 == 0 {
            encode_incr(&format!("ctr:{id}"))
        } else {
            encode_set(&key, &value)
        };

        if stream.write_all(&cmd).is_err() {
            write_errors.fetch_add(1, Ordering::Relaxed);
            if let Ok(s) = TcpStream::connect(MASTER) {
                stream = s;
                stream.set_nodelay(true).ok();
                stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            }
            continue;
        }

        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                write_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                write_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        counter += 1;
    }
}

fn reader_thread(
    id: usize,
    replica: &'static str,
    running: Arc<AtomicBool>,
    read_count: Arc<AtomicU64>,
    read_errors: Arc<AtomicU64>,
    read_hits: Arc<AtomicU64>,
) {
    let mut stream = match TcpStream::connect(replica) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Reader {id} connect to {replica} failed: {e}");
            return;
        }
    };
    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let mut buf = [0u8; 4096];
    let mut counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let writer_id = counter % WRITE_CLIENTS as u64;
        let key_num = (counter / WRITE_CLIENTS as u64) % 50;
        let key = format!("k:{writer_id}:{key_num}");
        
        let cmd = encode_get(&key);

        if stream.write_all(&cmd).is_err() {
            read_errors.fetch_add(1, Ordering::Relaxed);
            if let Ok(s) = TcpStream::connect(replica) {
                stream = s;
                stream.set_nodelay(true).ok();
                stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            }
            continue;
        }

        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                read_count.fetch_add(1, Ordering::Relaxed);
                if buf[0] != b'_' {
                    read_hits.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => {
                read_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        counter += 1;
    }
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║            REDIS STRESS TEST - MASTER + 4 REPLICAS               ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ Master:   {} (writes)                              ║", MASTER);
    println!("║ Replicas: 6390, 6391, 6392, 6393 (reads)                         ║");
    println!("║ Writers:  {} clients → master                                   ║", WRITE_CLIENTS);
    println!("║ Readers:  {} clients/replica × 4 = {} total                    ║", 
             READ_CLIENTS_PER_REPLICA, READ_CLIENTS_PER_REPLICA * 4);
    println!("║ Duration: {} seconds                                             ║", TEST_DURATION_SECS);
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let running = Arc::new(AtomicBool::new(true));
    let write_count = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(AtomicU64::new(0));
    let read_hits = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for i in 0..WRITE_CLIENTS {
        let r = running.clone();
        let wc = write_count.clone();
        let we = write_errors.clone();
        handles.push(thread::spawn(move || writer_thread(i, r, wc, we)));
    }

    for (ridx, replica) in REPLICAS.iter().enumerate() {
        for i in 0..READ_CLIENTS_PER_REPLICA {
            let r = running.clone();
            let rc = read_count.clone();
            let re = read_errors.clone();
            let rh = read_hits.clone();
            let id = ridx * READ_CLIENTS_PER_REPLICA + i;
            handles.push(thread::spawn(move || reader_thread(id, replica, r, rc, re, rh)));
        }
    }

    let start = Instant::now();
    let mut last_w = 0u64;
    let mut last_r = 0u64;

    for sec in 1..=TEST_DURATION_SECS {
        thread::sleep(Duration::from_secs(1));
        
        let w = write_count.load(Ordering::SeqCst);
        let r = read_count.load(Ordering::SeqCst);
        let we = write_errors.load(Ordering::SeqCst);
        let re = read_errors.load(Ordering::SeqCst);
        let hits = read_hits.load(Ordering::SeqCst);
        
        let w_rate = w.saturating_sub(last_w);
        let r_rate = r.saturating_sub(last_r);
        let total_rate = w_rate + r_rate;
        let hit_pct = if r > 0 { hits as f64 / r as f64 * 100.0 } else { 0.0 };
        
        println!(
            "[{:2}s] W:{:>6}k/s R:{:>6}k/s = {:>6}k/s | Tot: W:{:>7} R:{:>8} | Err: W:{} R:{} | Hit:{:.0}%",
            sec, w_rate/1000, r_rate/1000, total_rate/1000, w, r, we, re, hit_pct
        );
        
        last_w = w;
        last_r = r;
    }

    running.store(false, Ordering::SeqCst);
    for h in handles { h.join().ok(); }

    let elapsed = start.elapsed().as_secs_f64();
    let tw = write_count.load(Ordering::SeqCst);
    let tr = read_count.load(Ordering::SeqCst);
    let twe = write_errors.load(Ordering::SeqCst);
    let tre = read_errors.load(Ordering::SeqCst);
    let th = read_hits.load(Ordering::SeqCst);

    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                         FINAL RESULTS                            ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ Duration:       {:>8.2}s                                         ║", elapsed);
    println!("║ Total Writes:   {:>8} ({:>5} errors)                            ║", tw, twe);
    println!("║ Total Reads:    {:>8} ({:>5} errors)                            ║", tr, tre);
    println!("║ Read Hits:      {:>8} ({:>5.1}% hit rate)                        ║", th, 
             if tr > 0 { th as f64 / tr as f64 * 100.0 } else { 0.0 });
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ Write Rate:     {:>8.0} ops/sec                                  ║", tw as f64 / elapsed);
    println!("║ Read Rate:      {:>8.0} ops/sec                                  ║", tr as f64 / elapsed);
    println!("║ TOTAL RATE:     {:>8.0} ops/sec                                  ║", (tw + tr) as f64 / elapsed);
    println!("╚══════════════════════════════════════════════════════════════════╝");
}
