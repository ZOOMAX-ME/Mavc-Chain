module 0x2::X {
    struct S {}

    public fun foo() {}
}

module 0x2::M {
    use 0x2::X;

    struct S { f: X::S }

    fun foo() {
        X::foo()
    }
}
