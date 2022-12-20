pub struct Order {
    pub id: u64,
    pub order_type: OrderType,
    pub qty: f64,
    pub price: f64,
}

pub enum OrderType {
    LimitBuy,
    LimitSell,
}

pub struct OrderBook {
    bid: Vec<Order>,
    ask: Vec<Order>
}

impl OrderBook {

    pub fn new() -> OrderBook {
        OrderBook {
            ask: vec!(),
            bid: vec!()
        }
    }

    pub fn submit(&mut self, order: Order) -> Vec<OrderMatchResult> {
        //TODO implement
        return vec!()
    }
}

pub struct OrderMatchResult<'a> {
    id: u64,
    order: &'a Order,
    qty_filled: f64
}

#[cfg(test)]
mod tests {
    use crate::order_book::{Order, OrderBook};
    use crate::order_book::OrderType::{LimitBuy, LimitSell};

    #[test]
    fn matching_orders_match(){
        let buy_order = Order {
            id: 1,
            order_type: LimitBuy,
            qty: 10.0,
            price: 0.05
        };

        let sell_order = Order {
            id: 2,
            order_type: LimitSell,
            qty: 10.0,
            price: 0.049
        };

        let mut book = OrderBook::new();

        book.submit(buy_order);

        let order_match_result = book.submit(sell_order);

        assert_eq!(order_match_result.len(), 2);
    }

    #[test]
    fn not_matching_orders_dont_match(){
        let buy_order = Order {
            id: 1,
            order_type: LimitBuy,
            qty: 10.0,
            price: 0.05
        };

        let sell_order = Order {
            id: 2,
            order_type: LimitSell,
            qty: 10.0,
            price: 0.051
        };

        let mut book = OrderBook::new();

        book.submit(buy_order);

        let order_match_result = book.submit(sell_order);

        assert_eq!(order_match_result.len(), 0);
    }

}