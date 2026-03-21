package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class Position(asset: String, size: BigDecimal)

object Position {
  implicit val circeDecoder: Decoder[Position] = deriveDecoder
}
