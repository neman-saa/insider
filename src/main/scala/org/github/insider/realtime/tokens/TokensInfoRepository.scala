package org.github.insider.realtime.tokens

import cats.data.NonEmptyList

import java.time.Instant

trait TokensInfoRepository[F[_]] {
  def insert(tokens: NonEmptyList[TokenInfo]): F[Unit]
  def select(now: Instant): F[List[TokenInfo]]
}
