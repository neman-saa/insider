package org.github.insider.polymarket.domain

case class Order(side: Side, tokenId: String, amount: BigDecimal, price: BigDecimal)
