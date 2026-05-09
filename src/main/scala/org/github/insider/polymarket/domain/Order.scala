package org.github.insider.polymarket.domain

sealed trait Order {
  def side: Side
  def size: BigDecimal
  def price: BigDecimal
}

case class BuyOrder(size: BigDecimal, price: BigDecimal) extends Order {
  override val side: Side = Side.Buy
}
case class SellOrder(size: BigDecimal, price: BigDecimal) extends Order {
  override val side: Side = Side.Sell
}

object Order {
  import io.circe.{Decoder, Encoder}
  import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

  implicit val orderEncoder: Encoder[Order] = deriveEncoder
  implicit val orderDecoder: Decoder[Order] = deriveDecoder

  implicit val buyOrderEncoder: Encoder[BuyOrder] = deriveEncoder
  implicit val buyOrderDecoder: Decoder[BuyOrder] = deriveDecoder

  implicit val sellOrderEncoder: Encoder[SellOrder] = deriveEncoder
  implicit val sellOrderDecoder: Decoder[SellOrder] = deriveDecoder
}
