package org.github.insider.polymarket.repository

import cats.data.NonEmptyList
import org.github.insider.polymarket.domain.Market

trait Markets[F[_]] {

  def insert(markets: NonEmptyList[(String, Market)]): F[Int]

}
