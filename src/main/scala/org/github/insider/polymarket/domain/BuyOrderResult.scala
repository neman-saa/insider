package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class BuyOrderResult(
  amount: BigDecimal,
  totalPrice: BigDecimal
)

object BuyOrderResult {
  implicit val circeDecoder: Decoder[BuyOrderResult] = deriveDecoder
}
