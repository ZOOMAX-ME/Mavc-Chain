// valid, Receiving type with object type param

module a::m {
    use sui::{object, transfer::Receiving};

    struct S has key { id: object::UID }

    public entry fun yes(_: Receiving<S>) {}
}

module sui::object {
    struct UID has store {
        id: address,
    }
}

module sui::transfer {
    struct Receiving<phantom T: key> has drop {
        id: address,
    }
}
