package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.Tag

trait TagsClient[F[_]] {
  def getTags(limit: Int, offset: Int): F[List[Tag]]
}
