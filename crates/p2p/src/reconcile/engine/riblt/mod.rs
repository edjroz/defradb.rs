//! Rateless invertible Bloom lookup tables (RIBLT).

mod decoder;
mod encoder;
mod mapping;
mod symbol;
mod window;

pub use decoder::Decoder;
pub use encoder::Encoder;
pub use symbol::CodedSymbol;

#[cfg(test)]
#[path = "decoder_tests.rs"]
mod decoder_tests;

#[cfg(test)]
#[path = "encoder_tests.rs"]
mod encoder_tests;

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod mapping_tests;
