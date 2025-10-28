package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveUnwrappedDecoder

final case class Volume(value: BigDecimal) extends AnyVal

object Volume {
  implicit val circeDecoder: Decoder[Volume] = deriveUnwrappedDecoder
}
