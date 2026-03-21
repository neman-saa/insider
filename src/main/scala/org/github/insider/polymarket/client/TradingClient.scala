package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{BuyOrderResult, Position, SellOrderResult}

trait TradingClient[F[_]] {
  def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[Option[BuyOrderResult]]
  def sell(tokenId: String, entity: BigDecimal, minPrice: Option[BigDecimal]): F[Option[SellOrderResult]]
  def balance(): F[Option[BigDecimal]]
  def positions(): F[List[Position]]
}
