package org.github.insider.simulations

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.alchemy.repository.TradesRepository
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry, LeaderboardStrategy}
import org.github.insider.polymarket.repository.Markets

import java.time.{Instant, ZoneOffset}

class Simulator[F[_]: Async](
  leaderboard: LeaderboardStrategy[F],
  trades: TradesRepository[F],
  markets: Markets[F]
) {

  private type TokenState = (BigDecimal, Int, Instant)
  private type Tokens     = Map[String, TokenState]

  def simulate(maxForUser: BigDecimal, batchLength: Int, initialBalance: BigDecimal): F[BigDecimal] = {

    def isLeaderboardExpired(currentDate: Instant, lbLastUpdate: Instant): Boolean =
      currentDate.getEpochSecond - lbLastUpdate.getEpochSecond > 3600 * 24

    def activeTokens(tokens: Tokens, currentDate: Instant): Tokens =
      tokens.filter {
        case (_, (_, _, closeDate)) =>
          closeDate.getEpochSecond > currentDate.getEpochSecond
      }

    def totalTokenValue(tokens: Tokens): BigDecimal =
      tokens.values.map { case (amount, price, _) => amount * price }.sum

    def mergeTokens(oldTokens: Tokens, newTokens: Tokens): Tokens =
      oldTokens ++ newTokens.map {
        case (tokenId, (newAmount, newPrice, newDate)) =>
          val (oldAmount, _, _) = oldTokens.getOrElse(tokenId, (BigDecimal(0), 0, Instant.now))
          (tokenId, (newAmount + oldAmount, newPrice, newDate))
      }

    def recursion(
      currentBalance: BigDecimal,
      currentDate: Instant,
      tokens: Tokens,
      offset: Long,
      lbLastUpdate: Instant,
      leaderboardRef: Ref[F, Map[HexAddress, LeaderboardEntry]]
    ): F[BigDecimal] =
      if (isLeaderboardExpired(currentDate, lbLastUpdate))
        for {
          board <- leaderboard.load(currentDate)
          _     <- leaderboardRef.set(board)
          res   <- recursion(currentBalance, currentDate, tokens, offset, currentDate, leaderboardRef)
        } yield res
      else
        for {
          batch <- trades.getHistoricalTrades(offset, batchLength)

          res <-
            if (batch.isEmpty) {
              (currentBalance + totalTokenValue(tokens)).pure[F]
            } else {
              for {
                currentLeaderboard <- leaderboardRef.get

                filteredTrades = batch.filter(trade => currentLeaderboard.contains(HexAddress(trade.makerAddress)))

                totalScore = currentLeaderboard.values.map(_.score).sum

                validTokens   = activeTokens(tokens, currentDate)
                markedBalance = currentBalance + totalTokenValue(tokens.removedAll(validTokens.keys))

                purchasesFromTrades <- filteredTrades
                  .map(trade => (trade.tokenId, trade.makerAddress, trade.singleTokenPrice))
                  .traverse {
                    case (tokenId, makerAddress, singleTokenPrice) =>
                      for {
                        (closeDate, lastPrice) <- markets.getMarketClosedTimeWithLastPriceByTokenId(tokenId)
                        userScore               = currentLeaderboard(HexAddress(makerAddress)).score
                      } yield (userScore, closeDate, lastPrice, singleTokenPrice, tokenId)
                  }

                (newTokens, newBalance) = purchasesFromTrades.foldLeft(
                  (Map.empty[String, TokenState], markedBalance)
                ) {
                  case ((accTokens, balance), (userScore, closeDate, lastPrice, singleTokenPrice, tokenId)) =>
                    val cost =
                      scala.math.min((userScore / totalScore).toDouble, maxForUser.toDouble) * balance / 20
                    val tokenAmount    = cost / (singleTokenPrice + 0.01)
                    val updatedBalance = balance - cost
                    val updatedTokens = accTokens.updatedWith(tokenId) {
                      case Some((existingAmount, _, _)) =>
                        Some((existingAmount + tokenAmount, lastPrice, closeDate))
                      case None =>
                        Some((tokenAmount, lastPrice, closeDate))
                    }

                    (updatedTokens, updatedBalance)
                }

                finalTokens = mergeTokens(validTokens, newTokens)

                nextDate = batch
                  .flatMap(_.blockTimestamp)
                  .maxBy(_.toInstant(ZoneOffset.UTC))
                  .toInstant(ZoneOffset.UTC)

                res <- recursion(
                  newBalance,
                  nextDate,
                  finalTokens,
                  offset + batchLength,
                  lbLastUpdate,
                  leaderboardRef
                )
              } yield res
            }
        } yield res

    for {
      earliestTime       <- trades.getEarliestTradeTimestamp
      initialLeaderboard <- leaderboard.load(earliestTime)
      leaderboardRef     <- Ref.of[F, Map[HexAddress, LeaderboardEntry]](initialLeaderboard)
      res <- recursion(
        initialBalance,
        earliestTime,
        Map.empty,
        0L,
        earliestTime,
        leaderboardRef
      )
    } yield res
  }
}
