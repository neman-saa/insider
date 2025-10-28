package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.Event
import java.time.LocalDateTime

trait EventsClient[F[_]] {
  def getEvents(startDateMax: LocalDateTime, endDateMax: LocalDateTime): F[List[Event]]
}
