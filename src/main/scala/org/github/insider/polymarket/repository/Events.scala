package org.github.insider.polymarket.repository

import cats.data.NonEmptyList
import org.github.insider.polymarket.domain.Event

import java.time.Instant

trait Events[F[_]] {
  def insert(events: NonEmptyList[Event]): F[Int]

  def getLatestClosedDate: F[Instant]
}
