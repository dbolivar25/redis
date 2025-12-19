use clap::Parser;
use clap::Subcommand;

/// Redis Client CLI
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
pub struct Args {
    /// Host of the server
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port of the server
    #[clap(long, default_value_t = 6379)]
    pub port: u16,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Ping,
    Echo { message: String },
    Set {
        key: String,
        value: String,
        #[clap(subcommand)]
        ttl: Option<TTLOpt>,
    },
    Get { key: String },
    Del { key: String },
    Psync {
        repl_id: String,
        #[clap(allow_hyphen_values = true)]
        offset: String,
    },
    Incr { key: String },
    Decr { key: String },
    Incrby {
        key: String,
        #[clap(allow_hyphen_values = true)]
        delta: i64,
    },
    Decrby {
        key: String,
        #[clap(allow_hyphen_values = true)]
        delta: i64,
    },
    Append { key: String, value: String },
    Strlen { key: String },
    Exists { keys: Vec<String> },
    Keys { pattern: String },
    Rename { key: String, newkey: String },
    Type { key: String },
    Expire { key: String, seconds: u64 },
    Pexpire { key: String, milliseconds: u64 },
    Expireat { key: String, timestamp: u64 },
    Ttl { key: String },
    Pttl { key: String },
    Persist { key: String },
    Mget { keys: Vec<String> },
    Mset { pairs: Vec<String> },
    Dbsize,
    Flushdb,
}

/// Time-to-live options for SET command
#[derive(Subcommand, Debug)]
pub enum TTLOpt {
    /// Time to live in seconds
    Ex {
        /// The number of seconds to live
        seconds: u64,
    },

    /// Time to live in milliseconds
    Px {
        /// The number of milliseconds to live
        milliseconds: u64,
    },
}
