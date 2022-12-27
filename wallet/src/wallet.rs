use crate::wallet::WalletCommand::Block;

#[derive(Clone, Debug)]
pub struct Wallet {
    pub id: String,
    pub currency: String,
    pub credit: f64,
    pub blocked: f64
}

impl Wallet {

    pub fn new(id: String, currency: String) -> Wallet {
        Wallet {
            id,
            currency,
            credit: 0.0,
            blocked: 0.0
        }
    }

    pub fn apply(&self, command: &WalletCommand) -> Wallet {
        let x = command;

        match &command {
            Block { wallet_id, amount} => self.block(amount.clone()),
            _ => self.clone()
        }

    }


    pub fn rollback(&self, command: WalletCommand) -> Wallet {
        //TODO
        self.clone()
    }



    fn block(&self, amount: f64) -> Wallet {
        let mut wallet = self.clone();
        wallet.blocked += amount;
        wallet
    }

    fn unblock(&self, amount: f64) -> Wallet {
        let mut wallet = self.clone();
        wallet.blocked -= amount;
        wallet
    }


    fn credit(&self, amount: f64) -> Wallet {
        let mut wallet = self.clone();
        wallet.credit += amount;
        wallet
    }

    fn debit(&self, amount: f64) -> Wallet {
        let mut wallet = self.clone();
        wallet.credit -= amount;
        wallet
    }

    fn available(&self) -> f64 {
        self.credit - self.blocked
    }
}

pub enum WalletCommand {
    Block { wallet_id: String, amount: f64 },
    Unblock { wallet_id: String, amount: f64 },
    Debit { wallet_id: String, amount: f64 },
    Credit { wallet_id: String, amount: f64 }
}