use clap::Parser;

/// Redis Client TUI
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
pub struct Args {
    /// Host of the server
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port of the server
    #[clap(long, default_value_t = 6379)]
    pub port: u16,
}
