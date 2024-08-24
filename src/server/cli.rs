use clap::Parser;

/// Redis Server CLI
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
pub struct Args {
    /// Host to listen on
    #[clap(long, default_value = "localhost")]
    pub host: String,

    /// Port to listen on
    #[clap(long, default_value_t = 6379)]
    pub port: u16,

    /// Host and port of the master server delimited by ':'
    #[clap(long, default_value = None)]
    pub master: Option<String>,
}
