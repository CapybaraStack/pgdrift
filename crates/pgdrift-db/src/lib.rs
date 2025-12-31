pub mod connection;
pub mod discovery;
pub mod fixtures;
pub mod sampler;
pub mod test_utils; // Test utilities - available for integration tests

pub use connection::ConnectionPool;
pub use discovery::{JsonbColumn, discover_jsonb_columns};
pub use sampler::{Sampler, SamplingStrategy};
