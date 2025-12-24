package org.github.insider.polymarket.domain

case class Trade(
  sellerAddress: String,
  buyerAddress: String,
  assetId: String,
  size: BigDecimal,
  price: BigDecimal,
  timestamp: String
)


