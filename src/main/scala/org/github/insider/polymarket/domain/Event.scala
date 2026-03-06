package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import java.time.Instant

final case class Event(
  id: String,
  title: String,
  createdAt: Instant,
  closedTime: Option[Instant],
  volume: Option[Volume],
  markets: Option[List[Market]],
  tags: Option[List[Tag]]
)

object Event {
  implicit val circeDecoder: Decoder[Event] = deriveDecoder
}
