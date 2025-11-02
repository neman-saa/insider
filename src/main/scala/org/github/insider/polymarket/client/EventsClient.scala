package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.{Event, Tag}

trait EventsClient[F[_]] {
  def getEventsByTag(tag: Tag, limit: Int, offset: Int): F[List[Event]]
}
