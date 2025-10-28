package org.github.insider.polymarket.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import org.github.insider.polymarket.codecs.CustomCodecs._

import java.time.LocalDateTime

final case class Event(
  id: String,
  title: String,
  startDate: LocalDateTime,
  endDate: LocalDateTime,
  volume: Volume,
  markets: List[Market],
  createdAt: LocalDateTime,
)

object Event {
  implicit val circeDecoder: Decoder[Event] = deriveDecoder
}
