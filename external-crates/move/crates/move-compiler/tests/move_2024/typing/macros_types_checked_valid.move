module a::m {
    public struct None()
    public struct NeedsCopy<phantom T: copy> {} has copy, drop, store;

    macro fun foo<$T: copy, $U>($_a: u64, $_r: &mut u64, $_n: NeedsCopy<$T>) {
        let _: NeedsCopy<$U> = NeedsCopy {};
    }

    #[allow(dead_code)]
    fun t() {
        foo!<u64, NeedsCopy<bool>>(0, &mut 1, NeedsCopy {});
    }
}
