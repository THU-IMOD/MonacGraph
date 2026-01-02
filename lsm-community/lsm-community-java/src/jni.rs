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
pub extern "system" fn Java_com_graph_rocks_RustJNI_openDB(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getVertexHandleById(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getAllVertices(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getEdgeHandleById(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getAllEdges(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_closeDB(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_createVertex(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getDataFromVertexHandle(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_putVertexData(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_removeVertex(
    env: JNIEnv,
    _class: JObject,
    graph_handle: jlong,
    vertex_handle: jlong,
) {
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_graph_rocks_RustJNI_getEdgeHandleByVertex(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_createEdge(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_getDataFromEdgeHandle(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_putEdgeData(
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
pub extern "system" fn Java_com_graph_rocks_RustJNI_removeEdge(
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
