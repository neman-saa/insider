package org.github.insider.alchemy.client

import cats.effect.Ref
import cats.effect.kernel.Sync
import cats.syntax.all._

class ApiKeysPool[F[_]](pool: List[String], lastUsedIndexR: Ref[F, Int]) {

  def getApiKey: F[String] =
    lastUsedIndexR.modify { lastUsedIndex =>
      val nextIndex = if (lastUsedIndex >= pool.size - 1) 0 else lastUsedIndex + 1

      (nextIndex, pool.apply(nextIndex))
    }
}

object ApiKeysPool {
  def of[F[_]: Sync](pool: List[String]): F[ApiKeysPool[F]] =
    Ref.of[F, Int](0).map { lastUsedIndexR =>
      new ApiKeysPool[F](pool, lastUsedIndexR)
    }
}
