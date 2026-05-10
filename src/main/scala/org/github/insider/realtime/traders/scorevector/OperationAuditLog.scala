package org.github.insider.realtime.traders.scorevector

import org.github.insider.polymarket.domain.Side
import org.github.insider.polymarket.domain.Side.Sell
import org.github.insider.realtime.tokens.TokenId

import java.time.Instant

sealed trait OperationAuditLog {
  def tokenId: TokenId

  def momentScore: Option[BigDecimal]

  def price: BigDecimal

  def side: Side

  def timestamp: Instant
}

object OperationAuditLog {

  final case class BuyAuditLog(
    tokenId: TokenId,
    momentScore: Option[BigDecimal],
    price: BigDecimal,
    timestamp: Instant,
  ) extends OperationAuditLog {
    override def side: Side = Side.Buy
  }

  final case class SellAuditLog(
    tokenId: TokenId,
    momentScore: Option[BigDecimal],
    price: BigDecimal,
    timestamp: Instant,
  ) extends OperationAuditLog {
    override def side: Side = Sell
  }
}
