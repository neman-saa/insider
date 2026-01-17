package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class Token(
  outcome: Option[String],
  id: Option[String],
  lastPrice: Option[Volume]
)

object Token {
  import org.github.insider.polymarket.domain.Volume._
  implicit val circeDecoder: Decoder[Token] = deriveDecoder
}
