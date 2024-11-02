use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread_local;
use std::ptr;
use std::cell::RefCell;
use rand::{random, Rng};
use rand::rngs::ThreadRng;

use std::backtrace::Backtrace;
use std::collections::BTreeMap;
#[allow(deprecated)]
use std::panic::PanicInfo;
use std::thread;
use std::time::Duration;
use clap::Parser;
use crate::client::ExampleClient;
use crate::Request;
use std::process::Command;
use std::sync::RwLock;
use once_cell::sync::Lazy;
use maplit::btreemap;
pub use crate::typ::{RPCError, ClientWriteError, ClientWriteResponse};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead};
use anyhow::{Error, anyhow};

use maplit::btreeset;
use flexkv::BasicNode;
use crate::Node;
use crate::start_example_raft_node;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;
use tempfile;


// fn get_addr_pairs(node_id: u64) -> Result<(String, String), anyhow::Error> {
//     match node_id {
//         1 => Ok(("127.0.0.1:21001".to_string(), "127.0.0.1:22001".to_string())),
//         2 => Ok(("127.0.0.1:21002".to_string(), "127.0.0.1:22002".to_string())),
//         3 => Ok(("127.0.0.1:21003".to_string(), "127.0.0.1:22003".to_string())),
//         _ => Err(anyhow::anyhow!("node {} not found", node_id)),
//     }
// }

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct ServerOpt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,

    #[clap(long)]
    pub rpc_addr: String,
}

#[allow(deprecated)]
pub fn log_panic(panic: &PanicInfo) {
    let backtrace = {
        format!("{:?}", Backtrace::force_capture())
    };

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



thread_local! {
    static CLIENTS: RefCell<Vec<Option<ExampleClient>>> = RefCell::new(Vec::new());
    static RNG: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
    static RUNTIME: Runtime = Runtime::new().unwrap();
}


// Singleton global for storing node addresses.
static NODE_ADDR_MAP: Lazy<RwLock<HashMap<u64, (String, String)>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Parses the configuration file and initializes `NODE_ADDR_MAP` only once.
fn initialize_config(config_file: *const c_char) -> Result<(), Error> {
    let config_path = unsafe { CStr::from_ptr(config_file).to_str()? };
    let file = fs::File::open(config_path)?;
    let reader = io::BufReader::new(file);

    let mut addr_map = HashMap::new();

    // for line in reader.lines() {
    //     let line = line?;
    //     let parts: Vec<&str> = line.split('=').collect();
    //     if parts.len() == 2 {
    //         let node_id = parts[0].parse::<u64>().map_err(|_| anyhow::anyhow!("Invalid node ID"))?;
    //         let addrs: Vec<&str> = parts[1].split(':').collect();
    //         if addrs.len() == 2 {
    //             addr_map.insert(node_id, (addrs[0].to_string(), addrs[1].to_string()));
    //         }
    //     }
    // }

    for line in reader.lines() {
        let line = line?;
        // Split by comma and trim whitespace
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() == 3 {
            let node_id: u64 = parts[0].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid node ID")
            })?;
            println!("node id: {}, http_port: {}, rpc_port: {}", node_id, parts[1].to_string(),  parts[2].to_string() );
            addr_map.insert(node_id, (parts[1].to_string(), parts[2].to_string())); 
 
        } else {
            return Err(anyhow!("Invalid config line format {}", line));
        }
    }

    // for line in reader.lines() {
    //     let line = line?;
    //     let parts: Vec<&str> = line.split_whitespace().collect();
    //     if parts.len() >= 2 {
    //         let node_id: u64 = parts[0].parse().map_err(|_| {
    //             io::Error::new(io::ErrorKind::InvalidData, "Invalid node ID")
    //         })?;
    //         let addrs: Vec<&str> = parts[1].split(',').collect();
    //         if addrs.len() != 2 {
    //             // return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid address format"));
    //             return Err(anyhow!("Invalid address format for node {}", node_id));
    //         }
    //         addr_map.insert(node_id, (addrs[0].to_string(), addrs[1].to_string())); 
    //     }
    // }

    let mut map = NODE_ADDR_MAP.write().unwrap();
    *map = addr_map;

    Ok(())
}

/// Retrieves the API and RPC addresses for a given node ID from the singleton `NODE_ADDR_MAP`.
fn get_addr_pairs(node_id: u64) -> Result<(String, String), Error> {
    let map = NODE_ADDR_MAP.read().unwrap();
    map.get(&node_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))
}



fn generate_random_number(num:usize) -> usize {
    RNG.with(|rng| rng.borrow_mut().gen_range(0..=num))
}


#[no_mangle]
pub extern "C" fn kv739_init(config_file: *const c_char) -> c_int {

    if NODE_ADDR_MAP.read().unwrap().is_empty() {
        // Initialize only if the map is empty
        if let Err(e) = initialize_config(config_file) {
            eprintln!("Failed to parse config file: {}", e);
            return -1;
        } else {
            println!("Successfully parse config !!!");
        }
    }

    NODE_ADDR_MAP.read().unwrap().iter().for_each(|(&node_id, (api_addr, _rpc_addr))| {
        println!("api_addr is {}", api_addr);
        CLIENTS.with(|clients| {
            clients.borrow_mut().push(Some(ExampleClient::new(node_id, api_addr.clone())));
        });
        println!("Initialized client with id {}!!!", node_id);
    });
    // for node_id in 1..4 {
    //     match get_addr_pairs(node_id) {
    //         Ok((api_addr, _rpc_addr)) => {
    //             println!("api_addr is {}", api_addr);
    //             CLIENTS.with(|clients| {
    //                clients.borrow_mut().push(Some(ExampleClient::new(node_id, api_addr)));
    //             });
    //             println!("Initialized client with id {}!!!",  node_id);
    //         }
    //         Err(_) => {
    //             println!("Failed to get node addresses");
    //             return -1;
    //         }
    //     }
    // }

    println!("Initialized all clients for load-balance!");
    0
}




#[no_mangle]
pub extern "C" fn kv739_shutdown() -> c_int {
    CLIENTS.with(|clients| {
        clients.borrow_mut().clear();
    });
    println!("All clients shut down");
    0
}



fn get_client_in_round_robin<F, R>(mut action: F) -> Result<R, &'static str>
where
    F: FnMut(&ExampleClient) -> Result<R, &'static str>,
{
    CLIENTS.with(|clients| {
        let clients: std::cell::Ref<'_, Vec<Option<ExampleClient>>> = clients.borrow();
        let len = clients.len();
        let mut index = 0;
        let random_idx = generate_random_number(len - 1);

        while index < len {
            // let real_idx = (random_idx + index + 1) % len;
            let real_idx = index;
            if let Some(client) = &clients[real_idx] {
                if let Ok(result) = action(client) {
                    return Ok(result);
                }
            }
            index += 1;
        }
        Err("All clients failed to respond")
    })
}

#[no_mangle]
pub extern "C" fn kv739_get(key: *const c_char, value: *mut c_char) -> c_int {
    let key = unsafe { CStr::from_ptr(key) };
    match key.to_str() {
        Ok(key_str) => {
            let result = get_client_in_round_robin(|client_instance| {
                RUNTIME.with(|rt| {
                    rt.block_on(async {
                        match client_instance.consistent_read(&key_str.to_string()).await {
                            Ok(value_str) => {
                                println!("GET - Retrieved value: '{}' for key: '{}'", value_str, key_str);
                                if value_str.is_empty() {
                                    println!("GET - Key not found");
                                    return Ok(1); // Key not found
                                }

                                let c_value = match CString::new(value_str) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        println!("GET - Failed to create CString: {:?}", e);
                                        return Err("Failed to create CString");
                                    }
                                };
                                unsafe {
                                    ptr::copy_nonoverlapping(
                                        c_value.as_ptr(),
                                        value,
                                        c_value.to_bytes_with_nul().len(),
                                    );
                                }
                                Ok(0) // Success with value
                            }
                            Err(e) => {
                                println!("GET - Error: {:?}", e);
                                Err("Get failed")
                            }
                        }
                    })
                })
            });
            match result {
                Ok(status) => status,
                Err(e) => {
                    println!("GET - Fatal error: {:?}", e);
                    -1
                }
            }
        }
        Err(_) => -1,
    }
}


#[no_mangle]
pub extern "C" fn kv739_put(key: *const c_char, value: *const c_char, old_value: *mut c_char) -> c_int {
    let key = unsafe { CStr::from_ptr(key) };
    let value = unsafe { CStr::from_ptr(value) };
    match (key.to_str(), value.to_str()) {
        (Ok(key_str), Ok(value_str)) => {
            let result = get_client_in_round_robin(|client_instance| {
                RUNTIME.with(|rt| {
                    rt.block_on(async {
                        // First get the old value
                        let old_value_result = client_instance.consistent_read(&key_str.to_string()).await;
                        println!("PUT - Old value read result: {:?}", old_value_result);

                        // Perform the write operation
                        let req = Request::Set {
                            key: key_str.to_string(),
                            value: value_str.to_string(),
                        };

                        match old_value_result {
                            Ok(old) => {
                                if old.is_empty() {
                                    println!("PUT - No old value found, returning 1");
                                    Ok(1) // No old value existed
                                } else {
                                    match client_instance.write(&req).await {
                                        Ok(_) => {
                                            println!("PUT - Found old value: '{}'", old);
                                            let c_old = CString::new(old).map_err(|_| "Failed to create CString")?;
                                            unsafe {
                                                ptr::copy_nonoverlapping(
                                                    c_old.as_ptr(),
                                                    old_value,
                                                    c_old.to_bytes_with_nul().len(),
                                                );
                                            }
                                            Ok(0) // Success with old value
                                        }
                                        Err(e) => {
                                            println!("PUT - Error retrieving old value: {:?}", e);
                                            Err("Failed to retrieve old value")
                                        }

                                    }
            
                                }

                            } 
                            Err(e) => {
                                println!("PUT - Error retrieving old value: {:?}", e);
                                Err("Failed to retrieve old value")
                            }

                        }
            
                    })
                })
            });
            match result {
                Ok(status) => status,
                Err(_) => -2, // Communication failure
            }
        }
        _ => -1,
    }
}







#[no_mangle]
pub extern "C" fn kv739_die(instance_name: *const c_char, clean: c_int) -> c_int {
    let instance_name = match unsafe { CStr::from_ptr(instance_name) }.to_str() {
        Ok(name) => name,
        Err(_) => return -1,
    };

    let node_id = match get_node_id_from_addr(instance_name) {
        Some(id) => id,
        None => return -1,
    };

    let result = get_client_in_round_robin(|client_instance| {
        RUNTIME.with(|rt| {
            rt.block_on(async {
                if clean == 1 {
                    let mut current_members = btreeset! {};

                    match client_instance.metrics().await {
                        Ok(metrics) => {
                            let members = metrics.membership_config.membership()
                                .get_joint_config()[0].clone();
                            current_members.extend(members);
                        }
                        Err(_) => return Err("Failed to get metrics"),
                    }

                    current_members.remove(&node_id);

                    match client_instance.change_membership(&current_members).await {
                        Ok(_) => {
                            thread::sleep(Duration::from_millis(100));

                            let res = kill_process(instance_name);
                            if res == 0 {
                                Ok(0)
                            } else {
                                Err("Failed to kill process")
                            }
                        }
                        Err(_) => Err("Failed to change membership"),
                    }
                } else {
                    let res = kill_process(instance_name);
                    if res == 0 {
                        Ok(0)
                    } else {
                        Err("Failed to kill process")
                    }
                }
            })
        })
    });

    match result {
        Ok(status) => status,
        Err(_) => -1,
    }
}




#[no_mangle]
pub extern "C" fn kv739_start(instance_name: *const c_char, is_new: c_int) -> c_int {
    let instance_name = match unsafe { CStr::from_ptr(instance_name) }.to_str() {
        Ok(name) => name,
        Err(_) => return -1,
    };

    let node_id = match get_node_id_from_addr(instance_name) {
        Some(id) => id,
        None => return -1,
    };

    let result = get_client_in_round_robin(|client_instance| {
        RUNTIME.with(|rt| {
            rt.block_on(async {
                let (api_addr, rpc_addr) = match get_addr_pairs(node_id) {
                    Ok(addrs) => addrs,
                    Err(_) => return Err("Failed to get node addresses"),
                };

                match start_example_raft_node(
                    node_id,
                    format!("node{}.db", node_id),
                    api_addr.clone(),
                    rpc_addr.clone(),
                ).await {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(200)).await;

                        if is_new == 1 {
                            match client_instance.add_learner((node_id, api_addr, rpc_addr)).await {
                                Ok(_) => {
                                    let mut current_members = btreeset!{};
                                    match client_instance.metrics().await {
                                        Ok(metrics) => {
                                            let members = metrics.membership_config.membership()
                                                .get_joint_config()[0].clone();
                                            current_members.extend(members);
                                            current_members.insert(node_id);
                                            match client_instance.change_membership(&current_members).await {
                                                Ok(_) => Ok(0),
                                                Err(_) => Err("Failed to change membership"),
                                            }
                                        }
                                        Err(_) => Err("Failed to get metrics"),
                                    }
                                }
                                Err(_) => Err("Failed to add learner"),
                            }
                        } else {
                            let mut current_members = btreeset!{};
                            match client_instance.metrics().await {
                                Ok(metrics) => {
                                    let members = metrics.membership_config.membership()
                                        .get_joint_config()[0].clone();
                                    current_members.extend(members);
                                    current_members.insert(node_id);
                                    match client_instance.change_membership(&current_members).await {
                                        Ok(_) => Ok(0),
                                        Err(_) => Err("Failed to change membership"),
                                    }
                                }
                                Err(_) => Err("Failed to get metrics"),
                            }
                        }
                    }
                    Err(_) => Err("Failed to start node"),
                }
            })
        })
    });

    match result {
        Ok(status) => status,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn kv739_leave(instance_name: *const c_char, clean: c_int) -> c_int {
    let instance_name = match unsafe { CStr::from_ptr(instance_name) }.to_str() {
        Ok(name) => name,
        Err(_) => return -1,
    };

    let node_id = match get_node_id_from_addr(instance_name) {
        Some(id) => id,
        None => return -1,
    };

    let result = get_client_in_round_robin(|client_instance| {
        RUNTIME.with(|rt| {
            rt.block_on(async {
                let mut current_members = btreeset! {};
                match client_instance.metrics().await {
                    Ok(metrics) => {
                        let members = metrics.membership_config.membership()
                            .get_joint_config()[0].clone();
                        current_members.extend(members);
                        current_members.remove(&node_id);

                        if clean == 1 {
                            match client_instance.change_membership(&current_members).await {
                                Ok(_) => {
                                    thread::sleep(Duration::from_millis(100));
                                    Ok(0)
                                }
                                Err(_) => Err("Failed to change membership"),
                            }
                        } else {
                            match client_instance.change_membership(&current_members).await {
                                Ok(_) => Ok(0),
                                Err(_) => Err("Failed to change membership"),
                            }
                        }
                    }
                    Err(_) => Err("Failed to get metrics"),
                }
            })
        })
    });

    match result {
        Ok(status) => status,
        Err(_) => -1,
    }
}


    





    





fn kill_process(addr: &str) -> c_int {
    let port = match addr.split(':').last() {
        Some(p) => p,
        None => return -1,
    };

    let service_name = "flex";

   // Construct the command string
   let command_str = format!(
    "lsof -i :{} | grep \"{}\" | awk '{{print $2}}' | xargs kill -9",
    port, service_name
);

// Print the command
println!("Executing command: sh -c \"{}\"", command_str);
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "lsof -i :{} | grep \"{}\" | awk '{{print $2}}' | xargs kill -9",
            port, service_name
        ))
        .status();

    match status {
        Ok(exit_status) => {
            if exit_status.success() {
                0
            } else {
                -1
            }
        }
        Err(e) => {
            println!("Failed to execute command: {}", e);
            -1
        }
    }
}










fn get_node_id_from_addr(addr: &str) -> Option<u64> {
    match addr.split(':').last()?.parse::<u64>() {
        Ok(port) => match port {
            21001 => Some(1),
            21002 => Some(2),
            21003 => Some(3),
            _ => None,
        },
        Err(_) => None,
    }
}


#[no_mangle]
pub extern "C" fn kv739_launch_server(config_file: *const c_char) -> c_int {
    RUNTIME.with(|rt| {
        rt.block_on(async {
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

            let node_configs = vec![
                ServerOpt {
                    id: 1,
                    http_addr: "127.0.0.1:21001".to_string(),
                    rpc_addr: "127.0.0.1:22001".to_string(),
                },
                ServerOpt {
                    id: 2,
                    http_addr: "127.0.0.1:21002".to_string(),
                    rpc_addr: "127.0.0.1:22002".to_string(),
                },
                ServerOpt {
                    id: 3,
                    http_addr: "127.0.0.1:21003".to_string(),
                    rpc_addr: "127.0.0.1:22003".to_string(),
                },
            ];
            let service_name = "flexkv-rocks";
            for config in &node_configs {
                let executable_path = std::env::current_exe()
                    .expect("Failed to get current executable path")
                    .parent()
                    .expect("Failed to get parent directory")
                    .join(service_name);  

                let _child = Command::new(executable_path)
                    .arg("--id")
                    .arg(config.id.to_string())
                    .arg("--http-addr")
                    .arg(&config.http_addr)
                    .arg("--rpc-addr")
                    .arg(&config.rpc_addr)
                    .spawn()
                    .expect("Failed to spawn node process");
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;

            let client = ExampleClient::new(1, node_configs[0].http_addr.clone());

            println!("=== init single node cluster");
            client.init().await.unwrap();

            println!("=== metrics after init");
            let _x = client.metrics().await.unwrap();

            println!("=== add-learner 2");
            let _x = client.add_learner((2, 
                node_configs[1].http_addr.clone(), 
                node_configs[1].rpc_addr.clone()
            )).await.unwrap();

            println!("=== add-learner 3");
            let _x = client.add_learner((3, 
                node_configs[2].http_addr.clone(), 
                node_configs[2].rpc_addr.clone()
            )).await.unwrap();

            println!("=== metrics after add-learner");
            let x = client.metrics().await.unwrap();

            assert_eq!(&vec![btreeset![1]], x.membership_config.membership().get_joint_config());


            let nodes_in_cluster =
            x.membership_config.nodes().map(|(nid, node)| (*nid, node.clone())).collect::<BTreeMap<_, _>>();
            assert_eq!(
                btreemap! {
                    1 => Node{rpc_addr: get_addr_pairs(1).unwrap().1,  api_addr: get_addr_pairs(1).unwrap().0},
                    2 => Node{rpc_addr: get_addr_pairs(2).unwrap().1,  api_addr: get_addr_pairs(2).unwrap().0},
                    3 => Node{rpc_addr: get_addr_pairs(3).unwrap().1,  api_addr: get_addr_pairs(3).unwrap().0},
                },
                nodes_in_cluster
            );

            println!("=== change-membership to 1,2,3");
            let _x = client.change_membership(&btreeset! {1,2,3}).await.unwrap();

            println!("=== metrics after change-member");
            let x = client.metrics().await.unwrap();
            assert_eq!(
                &vec![btreeset![1, 2, 3]],
                x.membership_config.membership().get_joint_config()
            );
            println!("finish launching the cluster!");
            1
        })
    })
}

