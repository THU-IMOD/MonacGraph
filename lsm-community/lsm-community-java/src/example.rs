use lsm_community_java::{mapper::EdgeIdMapper, wrapper::LsmCommunityWrapper};
use serde_json::Value;
fn main() -> anyhow::Result<()> {
    // Create example graph for testing
    let graph_name = "example";
    let wrapper = LsmCommunityWrapper::create_example_for_test(graph_name)?;
    // Additional assertions and checks can be added here
    println!("Example graph '{}' created successfully.", graph_name);
    let vertex_property_v0_raw = wrapper.get_vertex_property(0)?;
    let json_value: Value = serde_json::from_slice(&vertex_property_v0_raw)?;

    let json_string = serde_json::to_string_pretty(&json_value)?;
    println!("Vertex 0 's properties: {:?}", json_string);

    let res_0_2 = EdgeIdMapper::pack_edge_handle(0u32, 2u32);
    println!("Packed edge handle for (0,2): {:?}", res_0_2);
    Ok(())
}
