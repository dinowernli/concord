#[path = "generated/keyvalue_proto.rs"]
pub mod keyvalue_proto;

mod store;
pub use store::MapStore;
pub use store::Store;
