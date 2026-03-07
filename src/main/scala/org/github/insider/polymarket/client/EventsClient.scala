package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{Event, Tag}

import java.time.Instant

trait EventsClient[F[_]] {
  def getEventsByTag(tag: Tag, limit: Int, offset: Int): F[List[Event]]
  def getEventsByMaxEndDate(maxEndDate: Instant, limit: Int, offset: Int): F[List[Event]]
  def getLastClosedEvents(limit: Int, offset: Int = 0): F[List[Event]]
}
