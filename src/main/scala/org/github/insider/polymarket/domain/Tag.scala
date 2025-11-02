package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class Tag(
  id: Int,
  label: Option[String],
  slug: Option[String],
)

object Tag {
  implicit val circeDecoder: Decoder[Tag] = deriveDecoder
}
