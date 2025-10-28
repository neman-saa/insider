package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class Token(
  outcome: Outcome,
  id: String,
)

object Token {
  implicit val circeDecoder: Decoder[Token] = deriveDecoder
}
