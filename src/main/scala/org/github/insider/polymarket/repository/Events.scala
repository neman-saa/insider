package org.github.insider.polymarket.repository

import org.github.insider.polymarket.domain.Event

trait Events[F[_]] {

  def insert(events: List[Event]): F[Int]

}
