package org.github.insider.polymarket.domain

import doobie.Meta
import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveUnwrappedDecoder

final case class Volume(value: BigDecimal) extends AnyVal

object Volume {
  implicit val circeDecoder: Decoder[Volume] = deriveUnwrappedDecoder
  implicit val metaVolume: Meta[Volume]      = Meta[BigDecimal].timap[Volume](x => Volume(x))(_.value)
}
