package org.github.insider.simulations

import cats.effect.{Async, Ref}
import org.github.insider.alchemy.repository.TradesRepository
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry, LeaderboardStrategy}
import org.github.insider.polymarket.domain.Token
import org.github.insider.polymarket.repository.Markets
import cats.syntax.all._

import java.time.Instant

class Simulator[F[_]: Async](leaderboard: LeaderboardStrategy[F], trades: TradesRepository[F], markets: Markets[F]) {
  def simulate(maxForUser: BigDecimal, batchLength: Int, initialBalance: BigDecimal): F[BigDecimal] = {
    def recursion(
      currentBalance: BigDecimal,
      currentDate: Instant,
      tokens: Map[Token, Instant],
      offset: Long,
      lbLastUpdate: Instant,
      leaderboardR: Ref[F, Map[HexAddress, LeaderboardEntry]]
    ): F[BigDecimal] =
      if(currentDate.getEpochSecond - lbLastUpdate.getEpochSecond > 3600 * 24)
        for {
          board <- leaderboard.load(currentDate)
          _ <- leaderboardR.set(board)
          res <- recursion(currentBalance, currentDate, tokens, offset, currentDate, leaderboardR)
        } yield res
      else
  }
}
