package org.github.insider.leaderboard

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import org.github.insider.alchemy.repository.TradesRepository
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

class Leaderboards[F[_]: Async, Entry <: LeaderboardEntry](
  strategy: LeaderboardStrategy[F, Entry],
  cache: Cache[F, LeaderboardKeyName, Map[HexAddress, Entry]],
  trades: TradesRepository[F]
)(logger: Logger[F]) {
  def getLeaderboard: F[Map[HexAddress, Entry]] =
    cache
      .getOrUpdate(strategy.key)(
        trades
          .getLatestBlock
          .flatMap(block => strategy.load(block, 2500).flatTap(_ => logger.info(s"Loaded ${strategy.key} into cache")))
      )
}

object Leaderboards {
  def make[F[_]: Async: Parallel, Entry <: LeaderboardEntry](
    strategy: LeaderboardStrategy[F, Entry],
    tradesRepository: TradesRepository[F]
  ): Resource[F, Leaderboards[F, Entry]] = {
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      cache <- Cache
        .expiring[F, LeaderboardKeyName, Map[HexAddress, Entry]](
          config = ExpiringCache.Config(
            expireAfterRead  = 120.minutes,
            expireAfterWrite = Some(120.minutes),
          )
        )
    } yield new Leaderboards[F, Entry](strategy, cache, tradesRepository)(logger)
  }
}
