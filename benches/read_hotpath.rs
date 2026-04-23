use std::path::Path;

use tokio::runtime::Runtime;

mod scenarios;

#[test]
#[hotpath::main]
pub fn read_hybrid_default_round_robin() {
    let r = Runtime::new().unwrap();
    r.block_on(async {
        let path = Path::new("/dev/zero");
        let size = 1024;
        let count = 100000;
        scenarios::read::read_hybrid_default_round_robin(path, size, count).await;
    });
}
