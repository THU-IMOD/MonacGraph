use dashmap::DashMap;
use jni::JNIEnv;
use jni::objects::{JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use lsm_storage::types::VId;
use once_cell::sync::Lazy;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::mapper::EdgeIdMapper;
use crate::wrapper::LsmCommunityWrapper;

/// Global registry to store LSMCommunity instances
/// Key: graph_handle (jlong), Value: Arc<LsmCommunityWrapper>
static GRAPH_REGISTRY: Lazy<DashMap<i64, Arc<LsmCommunityWrapper>>> = Lazy::new(DashMap::new);

/// Global counter for generating unique graph handles
static NEXT_GRAPH_HANDLE: AtomicI64 = AtomicI64::new(1);

/// Helper function to convert jbyteArray to Vec<u8>
fn jbytearray_to_vec(
    env: &mut JNIEnv,
    array: jbyteArray,
    error_context: &str,
) -> Result<Vec<u8>, ()> {
    let array_obj = unsafe { JByteArray::from_raw(array) };

    // Get array length
    let len = match env.get_array_length(&array_obj) {
        Ok(l) => l,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get {} array length: {}", error_context, e),
            );
            return Err(());
        }
    };

    // Allocate buffer
    let mut buf = vec![0i8; len as usize];

    // Copy data from Java array
    if let Err(e) = env.get_byte_array_region(&array_obj, 0, &mut buf) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to read {} byte array: {}", error_context, e),
        );
        return Err(());
    }

    // Convert i8 to u8
    Ok(buf.into_iter().map(|b| b as u8).collect())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_openDB(
    mut env: JNIEnv,
    _class: JObject,
    db_name: JString,
) -> jlong {
    // Convert JString to Rust String with proper error handling
    let db_name_rs: String = match env.get_string(&db_name) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid database name: {}", e),
            );
            return -1;
        }
    };

    // Create LSMCommunityWrapper instance
    let graph = match LsmCommunityWrapper::open(&db_name_rs) {
        Ok(g) => Arc::new(g),
        Err(e) => {
            let _ = env.throw_new(
                "java/io/IOException",
                format!("Failed to open database: {}", e),
            );
            return -1;
        }
    };

    // Generate unique handle
    let handle = NEXT_GRAPH_HANDLE.fetch_add(1, Ordering::SeqCst);

    // Store in global registry
    GRAPH_REGISTRY.insert(handle, graph);

    handle
}

use jni::objects::JByteArray;

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getVertexHandleById(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    outer_id: jbyteArray,
) -> jlong {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return -1;
        }
    };

    // Convert jbyteArray to JByteArray
    let outer_id_obj = unsafe { JByteArray::from_raw(outer_id) };

    // Convert jbyteArray to Vec<u8> with proper error handling
    let outer_id_bytes = {
        // Get array length
        let len = match env.get_array_length(&outer_id_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get array length: {}", e),
                );
                return -1;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&outer_id_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read byte array: {}", e),
            );
            return -1;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Get internal ID (only lookup, don't create)
    match graph.vertex_id_mapper.get_inner_id(&outer_id_bytes) {
        Some(inner_id) => inner_id as jlong,
        None => {
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getAllVertices(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Get all vertex inner IDs (0, 1, 2, ..., vertex_count - 1)
    let vertices = graph.get_all_vertices();
    let vertices: Vec<jlong> = vertices.into_iter().map(|id| id as jlong).collect();

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(vertices.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &vertices) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
#[allow(unused_variables)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getEdgeHandleById(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    outer_id: jbyteArray,
) -> jlong {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return -1;
        }
    };

    // Convert jbyteArray to JByteArray
    let outer_id_obj = unsafe { JByteArray::from_raw(outer_id) };

    // Convert jbyteArray to Vec<u8> with proper error handling
    let outer_id_bytes = {
        // Get array length
        let len = match env.get_array_length(&outer_id_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get array length: {}", e),
                );
                return -1;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&outer_id_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read byte array: {}", e),
            );
            return -1;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Get internal ID (only lookup, don't create)
    match graph.edge_id_mapper.get_inner_id(&outer_id_bytes) {
        Some(inner_id) => inner_id as jlong,
        None => {
            let _ = env.throw_new("java/util/NoSuchElementException", "Vertex not found");
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getAllEdges(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Read all edges from the graph
    let all_edges = graph.get_all_edges();

    // Convert edge pairs to packed i64 handles using EdgeIdMapper
    let edge_handles: Vec<jlong> = all_edges
        .into_iter()
        .map(|(src, dst)| EdgeIdMapper::pack_edge_handle(src, dst) as jlong)
        .collect();

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(edge_handles.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &edge_handles) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_closeDB(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) {
    // Remove the graph instance from the global registry
    match GRAPH_REGISTRY.remove(&graph_handle) {
        Some((_handle, graph_arc)) => {
            // Successfully removed from registry
            // The Arc will be dropped here, and if this is the last reference,
            // the LsmCommunityWrapper will be dropped and cleaned up
            drop(graph_arc);

            // Optional: Log success (you can remove this in production)
            #[cfg(debug_assertions)]
            eprintln!("Successfully closed graph with handle: {}", _handle);
        }
        None => {
            // Graph handle not found - throw exception
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!(
                    "Invalid graph handle: {} (already closed or never opened)",
                    graph_handle
                ),
            );
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_createVertex(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    outer_id: jbyteArray,
    data: jbyteArray,
) -> jlong {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return -1;
        }
    };

    // Convert outer_id jbyteArray to Vec<u8>
    let outer_id_obj = unsafe { JByteArray::from_raw(outer_id) };
    let outer_id_bytes = {
        // Get array length
        let len = match env.get_array_length(&outer_id_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get outer_id array length: {}", e),
                );
                return -1;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&outer_id_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read outer_id byte array: {}", e),
            );
            return -1;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Convert data jbyteArray to Vec<u8>
    let data_obj = unsafe { JByteArray::from_raw(data) };
    let data_bytes = {
        // Get array length
        let len = match env.get_array_length(&data_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get data array length: {}", e),
                );
                return -1;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&data_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read data byte array: {}", e),
            );
            return -1;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Call new_vertex on the graph wrapper
    match graph.new_vertex(&outer_id_bytes, &data_bytes) {
        Ok(inner_id) => inner_id as jlong,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create vertex: {}", e),
            );
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getDataFromVertexHandle(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
) -> jbyteArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Get vertex property (None already converted to empty Vec in wrapper)
    let bytes = match graph.get_vertex_property(vertex_handle as VId) {
        Ok(bytes) => bytes,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get vertex property: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    // Convert Vec<u8> to jbyteArray
    match env.byte_array_from_slice(&bytes) {
        Ok(array) => array.into_raw(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create byte array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_putVertexData(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
    data: jbyteArray,
) {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return;
        }
    };

    // Convert data jbyteArray to Vec<u8>
    let data_obj = unsafe { JByteArray::from_raw(data) };
    let data_bytes = {
        // Get array length
        let len = match env.get_array_length(&data_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get data array length: {}", e),
                );
                return;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&data_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read data byte array: {}", e),
            );
            return;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Put vertex property
    if let Err(e) = graph.put_vertex_property(vertex_handle as VId, &data_bytes) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to put vertex property: {}", e),
        );
    }
}

#[unsafe(no_mangle)]
#[allow(unused_variables)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_removeVertex(
    env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
) {
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getEdgeHandleByVertex(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
    direction: jint,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Validate direction (0 = out, 1 = in)
    if direction != 0 && direction != 1 {
        let _ = env.throw_new(
            "java/lang/IllegalArgumentException",
            format!(
                "Invalid direction: {} (must be 0 for out or 1 for in)",
                direction
            ),
        );
        return std::ptr::null_mut();
    }

    // Get neighbors
    let neighbors = match graph.get_neighbor(vertex_handle as VId, direction as u16) {
        Ok(neighbors) => neighbors,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get neighbors: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    // Convert to edge handles
    let source_vid = vertex_handle as VId;
    let edge_handles: Vec<jlong> = neighbors
        .into_iter()
        .map(|target_vid| {
            // Pack (source, target) into i64 edge handle based on direction
            let (from, to) = if direction == 0 {
                // Out direction: source -> target
                (source_vid, target_vid)
            } else {
                // In direction: target -> source
                (target_vid, source_vid)
            };
            
            // Pack two u32 values into one i64
            // High 32 bits: from, Low 32 bits: to
            let handle = EdgeIdMapper::pack_edge_handle(from, to);
            handle as jlong
        })
        .collect();

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(edge_handles.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &edge_handles) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_createEdge(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    outer_id: jbyteArray,
    src_vertex_handle: jlong,
    dst_vertex_handle: jlong,
    data: jbyteArray,
) -> jlong {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return -1;
        }
    };

    // Convert outer_id to Vec<u8>
    let outer_id_bytes = match jbytearray_to_vec(&mut env, outer_id, "outer_id") {
        Ok(bytes) => bytes,
        Err(_) => return -1,
    };

    // Convert edge property data to Vec<u8>
    let data_bytes = match jbytearray_to_vec(&mut env, data, "data") {
        Ok(bytes) => bytes,
        Err(_) => return -1,
    };

    // Create the edge
    match graph.new_edge(
        &outer_id_bytes,
        src_vertex_handle as VId,
        dst_vertex_handle as VId,
        &data_bytes,
    ) {
        Ok(()) => {
            // Return the packed edge handle
            EdgeIdMapper::pack_edge_handle(src_vertex_handle as u32, dst_vertex_handle as u32)
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create edge: {}", e),
            );
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getDataFromEdgeHandle(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    edge_handle: jlong,
) -> jbyteArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Unpack edge handle to get (src, dst)
    let (src, dst) = EdgeIdMapper::unpack_edge_handle(edge_handle);

    // Get edge property (None already converted to empty Vec in wrapper)
    let bytes = match graph.get_edge_property(src, dst) {
        Ok(bytes) => bytes,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get edge property: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    // Convert Vec<u8> to jbyteArray
    match env.byte_array_from_slice(&bytes) {
        Ok(array) => array.into_raw(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create byte array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_putEdgeData(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    edge_handle: jlong,
    data: jbyteArray,
) {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return;
        }
    };

    // Unpack edge handle to get (src, dst)
    let (src, dst) = EdgeIdMapper::unpack_edge_handle(edge_handle);

    // Convert data jbyteArray to Vec<u8>
    let data_obj = unsafe { JByteArray::from_raw(data) };
    let data_bytes = {
        // Get array length
        let len = match env.get_array_length(&data_obj) {
            Ok(l) => l,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to get data array length: {}", e),
                );
                return;
            }
        };

        // Allocate buffer
        let mut buf = vec![0i8; len as usize];

        // Copy data from Java array
        if let Err(e) = env.get_byte_array_region(&data_obj, 0, &mut buf) {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to read data byte array: {}", e),
            );
            return;
        }

        // Convert i8 to u8
        buf.into_iter().map(|b| b as u8).collect::<Vec<u8>>()
    };

    // Put edge property
    if let Err(e) = graph.lsm_community.put_edge_property(src, dst, &data_bytes) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to put edge property: {}", e),
        );
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_removeEdge(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    edge_handle: jlong,
) {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return;
        }
    };

    // Unpack edge handle to get (src, dst)
    let (src, dst) = EdgeIdMapper::unpack_edge_handle(edge_handle);

    // Remove the edge
    if let Err(e) = graph.remove_edge(src, dst) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to remove edge: {}", e),
        );
    }
}

/// Get all vertices reachable from a starting vertex using BFS
///
/// # Arguments
/// * `graph_handle` - Handle to the graph instance
/// * `vertex_handle` - Starting vertex for BFS traversal
///
/// # Returns
/// Array format: [vh1, dist1, vh2, dist2, ..., vhk, distk]
/// where vhi is the i-th reachable vertex handle and disti is its distance from start
#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getBfsVertices(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Validate vertex handle
    if vertex_handle < 0 || vertex_handle >= graph.vertex_count() as jlong {
        let _ = env.throw_new(
            "java/lang/IllegalArgumentException",
            format!("Invalid vertex handle: {}", vertex_handle),
        );
        return std::ptr::null_mut();
    }

    // Run BFS - returns Vec<(VId, u32)>
    let bfs_result = graph.lsm_community.bfs(vertex_handle as VId);

    // Convert Vec<(VId, u32)> to interleaved array [vh1, dist1, vh2, dist2, ...]
    let mut result = Vec::with_capacity(bfs_result.len() * 2);
    for (vertex_id, distance) in bfs_result {
        result.push(vertex_id as jlong);
        result.push(distance as jlong);
    }

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(result.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &result) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

/// Get Weakly Connected Components for the entire graph
///
/// # Arguments
/// * `graph_handle` - Handle to the graph instance
///
/// # Returns
/// Array format: [k, vh1, wcc1, vh2, wcc2, ..., vhn, wccn]
/// where k is the number of components, vhi is vertex handle i,
/// and wcci is the component ID (0 to k-1) for vertex i
#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getWCC(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Run WCC algorithm
    let wcc_result = graph.lsm_community.wcc();
    let vertex_count = wcc_result.len();

    // Count number of unique components and normalize component IDs to 0..k-1
    let component_map = DashMap::new();
    let mut next_component_id: VId = 0;

    let normalized_wcc: Vec<VId> = wcc_result
        .into_iter()
        .map(|comp_id| {
            *component_map.entry(comp_id).or_insert_with(|| {
                let id = next_component_id;
                next_component_id += 1;
                id
            })
        })
        .collect();

    let num_components = next_component_id;

    // Build result array: [k, vh0, wcc0, vh1, wcc1, ..., vh(n-1), wcc(n-1)]
    let mut result = Vec::with_capacity(2 * vertex_count + 1);
    result.push(num_components as jlong); // First element: number of components

    for (vertex_handle, component_id) in normalized_wcc.iter().enumerate() {
        result.push(vertex_handle as jlong); // Vertex handle
        result.push(*component_id as jlong); // Component ID (0 to k-1)
    }

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(result.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &result) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

/// Get Strongly Connected Components for the entire graph
///
/// # Arguments
/// * `graph_handle` - Handle to the graph instance
///
/// # Returns
/// Array format: [k, vh1, scc1, vh2, scc2, ..., vhn, sccn]
/// where k is the number of components, vhi is vertex handle i,
/// and scci is the component ID (0 to k-1) for vertex i
#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getSCC(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) -> jlongArray {
    // Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // Run SCC algorithm
    let normalized_scc = graph.lsm_community.scc();
    let vertex_count = normalized_scc.len();
    let num_components = normalized_scc.iter().max().unwrap() + 1;

    // Build result array: [k, vh0, scc0, vh1, scc1, ..., vh(n-1), scc(n-1)]
    let mut result = Vec::with_capacity(2 * vertex_count + 1);
    result.push(num_components as jlong); // First element: number of components

    for (vertex_handle, component_id) in normalized_scc.iter().enumerate() {
        result.push(vertex_handle as jlong); // Vertex handle
        result.push(*component_id as jlong); // Component ID (0 to k-1)
    }

    // Convert Vec<jlong> to jlongArray
    match env.new_long_array(result.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &result) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

/// Get pre-computed communities for the entire graph
///
/// # Arguments
/// * `graph_handle` - Handle to the graph instance
///
/// # Returns
/// Array format: [m, c1_size, v1_1, v1_2, ..., v1_c1, c2_size, v2_1, ..., v2_c2, ..., cm_size, vm_1, ..., vm_cm]
/// where:
/// - m is the number of communities
/// - ci_size is the size of the i-th community
/// - vi_1, vi_2, ..., vi_ci are the vertex IDs belonging to the i-th community
#[unsafe(no_mangle)]
pub extern "system" fn Java_db_monacgraph_jni_RustJNI_getCommunities(
    mut env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
) -> jlongArray {
    // 1. Get graph instance from global registry
    let graph = match GRAPH_REGISTRY.get(&graph_handle) {
        Some(entry) => Arc::clone(entry.value()),
        None => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid graph handle: {}", graph_handle),
            );
            return std::ptr::null_mut();
        }
    };

    // 2. Get pre-computed community structure via community_detection()
    let communities = graph.lsm_community.community_detection();
    let num_communities = communities.len();

    // 3. Calculate total capacity of the result vector to avoid reallocation
    let mut total_capacity = 1;
    for comm in &communities {
        total_capacity += 1 + comm.len();
    }

    // 4. Build the result vector
    let mut result = Vec::with_capacity(total_capacity);
    result.push(num_communities as jlong);

    for comm in &communities {
        result.push(comm.len() as jlong);
        for &vertex_id in comm {
            result.push(vertex_id as jlong);
        }
    }

    // 5. Convert Vec<jlong> to jlongArray
    match env.new_long_array(result.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_long_array_region(&array, 0, &result) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set array region: {}", e),
                );
                return std::ptr::null_mut();
            }
            array.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/OutOfMemoryError",
                format!("Failed to allocate array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

