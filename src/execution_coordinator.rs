use crate::order_book::{Order, OrderBook, OrderType};
use crate::order_book::OrderType::{LimitBuy, LimitSell};
use crate::wallet::Wallet;

pub fn test(book: &mut OrderBook){


    let mut wallet_1 = Wallet::new("1".to_string());
    wallet_1.currency.insert("BTC".to_string(), 1.0);

    let order_from_wallet_1 = Order {
        id: 1,
        order_type: LimitSell,
        price: 0.2,
        qty: 1.0
    };

    let mut wallet_2 = Wallet::new("2".to_string());
    wallet_2.currency.insert("ETH".to_string(), 5.0);

    let order_from_wallet_2 = Order {
        id: 2,
        order_type: LimitBuy,
        price: 0.2,
        qty: 5.0
    };

    //1. block corresponding qty of assets in order
    let orders_to_exec = book.submit(order_from_wallet_1); //orderBook has changed

    //if orders_to_exec contain something:
    //2. debit asset wallet sells (was blocked), 2.1 credit asset to other wallet (which buys)
    //3. credit asset wallet buys, 3.1 debit asset from other wallet (was blocked)
    //4. mark orders executed, maybe partially
    //5. assets block removed
    //6. order book changed
    // all above should be made in a single transaction

}