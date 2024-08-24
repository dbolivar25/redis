use clap::Parser;
use clap::Subcommand;

/// Redis Client CLI
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
pub struct Args {
    /// Host of the server
    #[clap(long, default_value = "localhost")]
    pub host: String,

    /// Port of the server
    #[clap(long, default_value_t = 6379)]
    pub port: u16,

    #[command(subcommand)]
    pub command: Commands,
}

/// Supported Redis commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Ping the server
    Ping,

    /// Echo a message
    Echo {
        /// The message to echo
        message: String,
    },

    /// Set a key-value pair
    Set {
        /// The key to set
        key: String,
        /// The value to set
        value: String,
        /// Time-to-live options
        #[clap(subcommand)]
        ttl: Option<TTLOpt>,
    },

    /// Get a key
    Get {
        /// The key to get
        key: String,
    },

    /// Delete a key
    Del {
        /// The key to delete
        key: String,
    },
}

/// Time-to-live options for SET command
#[derive(Subcommand, Debug)]
pub enum TTLOpt {
    /// Time to live in seconds
    Ex { seconds: i64 },

    /// Time to live in milliseconds
    Px { milliseconds: i64 },
}
