package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{BuyOrderResult, SellOrderResult}

trait TradingClient[F[_]] {
  def buy(tokenId: String, amount: BigDecimal, maxPrice: Option[BigDecimal]): F[Option[BuyOrderResult]]
  def sell(tokenId: String, amount: BigDecimal, minPrice: Option[BigDecimal]): F[Option[SellOrderResult]]
}
