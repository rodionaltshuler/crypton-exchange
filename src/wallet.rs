use std::collections::HashMap;

pub struct Wallet {
    pub id: String,
    pub currency: HashMap<String, f64>
}

impl Wallet {

    pub fn new(id: String) -> Wallet {
        Wallet {
            id,
            currency: HashMap::new()
        }
    }

}