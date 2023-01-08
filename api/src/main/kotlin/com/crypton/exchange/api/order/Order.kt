package com.crypton.exchange.api.order

import com.fasterxml.jackson.annotation.JsonAlias

data class Order(
    @JsonAlias("ID") val id: String,
    @JsonAlias("BASEASSETID") val baseAssetId: String,
    @JsonAlias("QUOTEASSETID") val quoteAssetId: String,
    @JsonAlias("WALLETID") val walletId: String,
    @JsonAlias("ORDERTYPE") val orderType: String,
    @JsonAlias("PRICE") val price: Double,
    @JsonAlias("QTY") val qty: Double,
    @JsonAlias("QTYFILLED") val qtyFilled: Double,
    @JsonAlias("STATUS") val status: String
)
