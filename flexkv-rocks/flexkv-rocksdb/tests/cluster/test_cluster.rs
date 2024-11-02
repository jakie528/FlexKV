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
            1 => "127.0.0.1:31001".to_string(),
            2 => "127.0.0.1:31002".to_string(),
            3 => "127.0.0.1:31003".to_string(),
            _ => panic!("node not found"),
        }
    }
    fn get_rpc_addr(node_id: u32) -> String {
        match node_id {
            1 => "127.0.0.1:32001".to_string(),
            2 => "127.0.0.1:32002".to_string(),
            3 => "127.0.0.1:32003".to_string(),
            _ => panic!("node not found"),
        }
    }

    let d1 = tempfile::TempDir::new()?;
    let d2 = tempfile::TempDir::new()?;
    let d3 = tempfile::TempDir::new()?;

    let handle = Handle::current();
    let handle_clone = handle.clone();
    let _h1 = thread::spawn(move || {
        let x = handle_clone.block_on(start_example_raft_node(1, d1.path(), get_addr(1), get_rpc_addr(1)));
        println!("x: {:?}", x);
    });

    let handle_clone = handle.clone();
    let _h2 = thread::spawn(move || {
        let x = handle_clone.block_on(start_example_raft_node(2, d2.path(), get_addr(2), get_rpc_addr(2)));
        println!("x: {:?}", x);
    });

    let _h3 = thread::spawn(move || {
        let x = handle.block_on(start_example_raft_node(3, d3.path(), get_addr(3), get_rpc_addr(3)));
        println!("x: {:?}", x);
    });

    tokio::time::sleep(Duration::from_millis(3_000)).await;


    let leader = ExampleClient::new(1, get_addr(1));


    println!("=== init single node cluster");
    leader.init().await?;

    println!("=== metrics after init");
    let _x = leader.metrics().await?;


    println!("=== add-learner 2");
    let _x = leader.add_learner((2, get_addr(2), get_rpc_addr(2))).await?;

    println!("=== add-learner 3");
    let _x = leader.add_learner((3, get_addr(3), get_rpc_addr(3))).await?;

    println!("=== metrics after add-learner");
    let x = leader.metrics().await?;

    assert_eq!(&vec![btreeset![1]], x.membership_config.membership().get_joint_config());

    let nodes_in_cluster =
        x.membership_config.nodes().map(|(nid, node)| (*nid, node.clone())).collect::<BTreeMap<_, _>>();
    assert_eq!(
        btreemap! {
            1 => Node{rpc_addr: get_rpc_addr(1), api_addr: get_addr(1)},
            2 => Node{rpc_addr: get_rpc_addr(2), api_addr: get_addr(2)},
            3 => Node{rpc_addr: get_rpc_addr(3), api_addr: get_addr(3)},
        },
        nodes_in_cluster
    );


    println!("=== change-membership to 1,2,3");
    let _x = leader.change_membership(&btreeset! {1,2,3}).await?;


    println!("=== metrics after change-member");
    let x = leader.metrics().await?;
    assert_eq!(
        &vec![btreeset![1, 2, 3]],
        x.membership_config.membership().get_joint_config()
    );


    println!("=== write `foo=bar`");
    let _x = leader
        .write(&Request::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        })
        .await?;


    tokio::time::sleep(Duration::from_millis(500)).await;


    println!("=== read `foo` on node 1");
    let x = leader.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 2");
    let client2 = ExampleClient::new(2, get_addr(2));
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 3");
    let client3 = ExampleClient::new(3, get_addr(3));
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);


    println!("=== read `foo` on node 2");
    let _x = client2
        .write(&Request::Set {
            key: "foo".to_string(),
            value: "wow".to_string(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;


    println!("=== read `foo` on node 1");
    let x = leader.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 2");
    let client2 = ExampleClient::new(2, get_addr(2));
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 3");
    let client3 = ExampleClient::new(3, get_addr(3));
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 1");
    let x = leader.consistent_read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 2 MUST return CheckIsLeaderError");
    let x = client2.consistent_read(&("foo".to_string())).await;
    match x {
        Err(e) => {
            let s = e.to_string();
            let expect_err:String = "error occur on remote peer 2: has to forward request to: Some(1), Some(Node { rpc_addr: \"127.0.0.1:32001\", api_addr: \"127.0.0.1:31001\" })".to_string();

            assert_eq!(s, expect_err);
        }
        Ok(_) => panic!("MUST return CheckIsLeaderError"),
    }

    Ok(())
}
