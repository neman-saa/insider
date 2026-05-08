package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{BuyOrderResult, Order, Position, SellOrderResult}

trait TradingClient[F[_]] {
  def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[BuyOrderResult]
  def sell(tokenId: String, shares: BigDecimal, minPrice: Option[BigDecimal]): F[SellOrderResult]
  def balance(): F[BigDecimal]
  def positions(user: Option[String] = None): F[List[Position]]
  def buyOrder(tokenId: String, amount: BigDecimal, price: BigDecimal): F[Unit]
  def sellOrder(tokenId: String, shares: BigDecimal, price: BigDecimal): F[Unit]
  def portfolioValue(user: Option[String] = None): F[BigDecimal]
  def ordersList(): F[List[Order]]
}
