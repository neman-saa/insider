package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class TotalPortfolioValueResult(user: String, value: BigDecimal)

object TotalPortfolioValueResult {
  implicit val circeDecoder: Decoder[TotalPortfolioValueResult] = deriveDecoder
}
