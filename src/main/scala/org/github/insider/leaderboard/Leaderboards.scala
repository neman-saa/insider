package org.github.insider.leaderboard

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

class Leaderboards[F[_]: Async](
  strategies: List[LeaderboardStrategy[F]],
  cache: Cache[F, LeaderboardKeyName, Map[HexAddress, LeaderboardEntry]],
)(logger: Logger[F]) {

  def find(address: HexAddress): F[List[(LeaderboardKeyName, LeaderboardEntry)]] =
    strategies
      .traverse { strategy =>
        cache
          .getOrUpdate(strategy.key)(strategy.load(Instant.now(), 1000).flatTap(_ => logger.info(s"Loaded ${strategy.key} into cache")))
          .map { leaderboardEntries =>
            leaderboardEntries.get(address).map(strategy.key -> _)
          }
      }
      .map(_.flatten)

  def getLeaderboard(leaderboardKeyName: LeaderboardKeyName): F[Option[Map[HexAddress, LeaderboardEntry]]] =
    cache.get(leaderboardKeyName)
}

object Leaderboards {
  def make[F[_]: Async: Parallel](
    strategies: List[LeaderboardStrategy[F]]
  ): Resource[F, Leaderboards[F]] = {
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      cache <- Cache
        .expiring[F, LeaderboardKeyName, Map[HexAddress, LeaderboardEntry]](
          config = ExpiringCache.Config(
            expireAfterRead  = 10.minutes,
            expireAfterWrite = Some(10.minutes),
          )
        )
    } yield new Leaderboards[F](strategies, cache)(logger)
  }
}
