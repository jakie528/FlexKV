use std::backtrace::Backtrace;
use std::collections::BTreeMap;
#[allow(deprecated)]
use std::panic::PanicInfo;
use std::thread;
use std::time::Duration;

use maplit::btreemap;
use maplit::btreeset;
use flex_kv_store::client::ExampleClient;
use flex_kv_store::start_example_raft_node;
use flex_kv_store::store::Request;
use flex_kv_store::Node;
use tokio::runtime::Handle;
use tracing_subscriber::EnvFilter;

#[allow(deprecated)]
pub fn log_panic(panic: &PanicInfo) {
    let backtrace = { format!("{:?}", Backtrace::force_capture()) };

    eprintln!("{}", panic);

    if let Some(location) = panic.location() {
        tracing::error!(
            message = %panic,
            backtrace = %backtrace,
            panic.file = location.file(),
            panic.line = location.line(),
            panic.column = location.column(),
        );
        eprintln!("{}:{}:{}", location.file(), location.line(), location.column());
    } else {
        tracing::error!(message = %panic, backtrace = %backtrace);
    }

    eprintln!("{}", backtrace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster() -> Result<(), Box<dyn std::error::Error>> {

    std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));

    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    fn get_addr(node_id: u32) -> String {
        match node_id {
            1 => "127.0.0.1:21001".to_string(),
            2 => "127.0.0.1:21002".to_string(),
            3 => "127.0.0.1:21003".to_string(),
            _ => panic!("node not found"),
        }
    }
    fn get_rpc_addr(node_id: u32) -> String {
        match node_id {
            1 => "127.0.0.1:22001".to_string(),
            2 => "127.0.0.1:22002".to_string(),
            3 => "127.0.0.1:22003".to_string(),
            _ => panic!("node not found"),
        }
    }

    println!("=== read `foo` on node 2");
    let client2 = ExampleClient::new(2, get_addr(2));
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);












    Ok(())
}
