use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub mod functional;

type Cache = Arc<Mutex<HashMap<String, Vec<u8>>>>;
