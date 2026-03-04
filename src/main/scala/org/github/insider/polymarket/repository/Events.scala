package org.github.insider.polymarket.repository

import org.github.insider.polymarket.domain.Event

import java.time.Instant

trait Events[F[_]] {

  def insert(events: List[Event]): F[Int]
  def getLatestClosedDate: F[Instant]

}
