package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{BuyOrderResult, SellOrderResult}

trait TradingClient[F[_]] {
  def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[Option[BuyOrderResult]]
  def sell(tokenId: String, entity: BigDecimal, minPrice: Option[BigDecimal]): F[Option[SellOrderResult]]
}
