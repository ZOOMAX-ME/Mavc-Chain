module 0x42::m {
    public enum Entry has drop {
        E(u64),
    }

    fun main() {
        let _x = Entry::E;
        let _x = Entry::E { x: 32 };
    }
}
