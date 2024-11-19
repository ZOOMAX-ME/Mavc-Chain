module 0x42::m {
    public enum E {
        X { x: u64 },
        Y { y: u64, x: u32 },
    }

    public fun test(e: &E): u64 {
        match (e) {
            E::X { x } | E::Y { y: _, x } => *x,
        }
    }
}
