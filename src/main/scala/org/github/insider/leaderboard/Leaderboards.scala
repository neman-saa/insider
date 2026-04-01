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
  strategies: List[LeaderboardStrategy[F, Entry]],
  cache: Cache[F, LeaderboardKeyName, Map[HexAddress, Entry]],
  trades: TradesRepository[F]
)(logger: Logger[F]) {

  def find(address: HexAddress): F[List[(LeaderboardKeyName, Entry)]] =
    strategies
      .traverse { strategy =>
        cache
          .getOrUpdate(strategy.key)(
            trades
              .getLatestBlock
              .flatMap(block =>
                strategy.load(block, 1000).flatTap(_ => logger.info(s"Loaded ${strategy.key} into cache"))
              )
          )
          .map { leaderboardEntries =>
            leaderboardEntries.get(address).map(strategy.key -> _)
          }
      }
      .map(_.flatten)

  def getLeaderboard(leaderboardKeyName: LeaderboardKeyName): F[Option[Map[HexAddress, Entry]]] =
    cache.get(leaderboardKeyName)
}

object Leaderboards {
  def make[F[_]: Async: Parallel, Entry <: LeaderboardEntry](
    strategies: List[LeaderboardStrategy[F, Entry]],
    tradesRepository: TradesRepository[F]
  ): Resource[F, Leaderboards[F, Entry]] = {
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      cache <- Cache
        .expiring[F, LeaderboardKeyName, Map[HexAddress, Entry]](
          config = ExpiringCache.Config(
            expireAfterRead  = 10.minutes,
            expireAfterWrite = Some(10.minutes),
          )
        )
    } yield new Leaderboards[F, Entry](strategies, cache, tradesRepository)(logger)
  }
}
