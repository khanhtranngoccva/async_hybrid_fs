mod spawnable_io;
pub(crate) use spawnable_io::SpawnablePendingIo;
use std::fmt::Debug;

pub trait Spawnable: Debug + Send + Sync {
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) -> ();
}

impl Spawnable for rayon::ThreadPool {
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        rayon::spawn(f);
    }
}

impl Spawnable for tokio::runtime::Handle {
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        self.spawn_blocking(f);
    }
}

impl Spawnable for tokio::runtime::Runtime {
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        self.spawn_blocking(f);
    }
}
