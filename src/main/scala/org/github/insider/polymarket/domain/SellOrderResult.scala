package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class SellOrderResult(
  amount: BigDecimal,
  totalPrice: BigDecimal
)

object SellOrderResult {
  implicit val circeDecoder: Decoder[SellOrderResult] = deriveDecoder
}
