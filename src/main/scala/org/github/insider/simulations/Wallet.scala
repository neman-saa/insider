package org.github.insider.simulations

import cats.effect.Sync
import cats.effect.std.{Random, UUIDGen}
import cats.syntax.all._
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry}

import java.time.Instant
import scala.math._

final case class Wallet(
  id: String,
  initialBalance: BigDecimal,
  lockedBalance: BigDecimal,
  freeBalance: BigDecimal,
  tokens: Map[(HexAddress, String), LeaderFollowingEntry],
  activeFromBlock: Int,
  activeToBlock: Option[Int],
) { self =>

  /** Returns updated wallet if buy succeeds, otherwise returns None */
  def copyBuy(
    tokenId: String,
    leader: HexAddress,
    amount: BigDecimal,
    totalPrice: BigDecimal,
    leaderboardEntry: LeaderboardEntry,
  )(config: SimulationConfig): Option[Wallet] = {
    tokens.get((leader, tokenId)) match {
      case None =>
        val singleTokenPrice = totalPrice / amount + 0.01
        val ourFirstPrice =
          totalPrice /
            leaderboardEntry.avgBuy *
            leaderboardEntry.score /
            leaderboardEntry.totalLeaderboardScore *
            leaderboardEntry.totalLeaderboardSize *
            freeBalance *
            config.extraBuyPerCents / 100
        val allowedPrice = freeBalance * config.allowedPerCentsPerUser / 100

        val ourFirstPricePutIn = ourFirstPrice min allowedPrice
        val leaderFollowingEntry = LeaderFollowingEntry(
          leader,
          totalPrice,
          ourFirstPrice,
          ourFirstPrice,
          ourFirstPricePutIn,
          allowedPrice,
          ourFirstPricePutIn / singleTokenPrice
        )
        val newTokens        = tokens + ((leader, tokenId) -> leaderFollowingEntry)
        val newFreeBalance   = freeBalance - allowedPrice
        val newLockedBalance = lockedBalance + allowedPrice - ourFirstPricePutIn
        Some(this.copy(tokens = newTokens, freeBalance = newFreeBalance, lockedBalance = newLockedBalance))

      case Some(followingEntry) =>
        val ourNewBuy          = followingEntry.ourFirstPrice * totalPrice / followingEntry.leaderFirstBuy
        val singleTokenPrice   = totalPrice / amount + 0.01
        val ourTotalPrice      = followingEntry.ourTotalPrice + ourNewBuy
        val ourTotalPricePutIn = ourTotalPrice min followingEntry.allowedTotalPrice
        val ourAmount =
          followingEntry.ourAmount + (ourTotalPricePutIn - followingEntry.ourTotalPricePutIn) / singleTokenPrice
        val newEntry = LeaderFollowingEntry(
          leader,
          followingEntry.leaderFirstBuy,
          followingEntry.ourFirstPrice,
          ourTotalPrice,
          ourTotalPricePutIn,
          followingEntry.allowedTotalPrice,
          ourAmount
        )
        val newLockedBalance = lockedBalance - (ourTotalPricePutIn - followingEntry.ourTotalPricePutIn)
        Some(this.copy(tokens = tokens + ((leader, tokenId) -> newEntry), lockedBalance = newLockedBalance))
    }
  }

  /** Returns updated wallet if sell succeeds, otherwise returns None */
  def copySell(tokenId: String, leader: HexAddress, amount: BigDecimal, totalPrice: BigDecimal): Option[Wallet] = {
    tokens.get((leader, tokenId)) match {
      case None => None
      case Some(followingEntry) =>
        val singleTokenPrice   = totalPrice / amount - 0.01
        val ourPrice           = followingEntry.ourFirstPrice * totalPrice / followingEntry.leaderFirstBuy
        val ourTotalPrice      = followingEntry.ourTotalPrice - ourPrice
        val ourTotalPricePutIn = ourTotalPrice min followingEntry.allowedTotalPrice
        val ourAmount =
          followingEntry.ourAmount - (followingEntry.ourTotalPricePutIn - ourTotalPricePutIn) / singleTokenPrice
        val newLockedBalance = lockedBalance - ourTotalPricePutIn + followingEntry.ourTotalPricePutIn
        val entry = LeaderFollowingEntry(
          leader,
          followingEntry.leaderFirstBuy,
          followingEntry.ourFirstPrice,
          ourTotalPrice,
          ourTotalPricePutIn,
          followingEntry.allowedTotalPrice,
          ourAmount
        )
        Some(this.copy(tokens = tokens + ((leader, tokenId) -> entry), lockedBalance = newLockedBalance))
    }
  }

  def resolveTokens(
    tokenResolutions: Map[TokenId, TokenResolutionInfo],
    resolvedBefore: Option[Instant],
    lockProfit: Boolean,
  ): Wallet = {
    val tokensToBeRemoved: List[(HexAddress, TokenId, LeaderFollowingEntry, TokenResolutionInfo)] =
      tokens.toList.flatMap {
        case ((makerAddress, tokenId), leaderEntry) =>
          tokenResolutions
            .get(tokenId)
            .filter { resolutionInfo =>
              resolvedBefore.forall(timestamp => resolutionInfo.resolveDate isAfter timestamp)
            }
            .map { resolutionInfo =>
              (makerAddress, tokenId, leaderEntry, resolutionInfo)
            }
      }

    val profit: BigDecimal = tokensToBeRemoved.map {
      case (_, _, leaderEntry, resolutionInfo) =>
        leaderEntry.ourAmount * resolutionInfo.lastPrice // already normalized ???
    }.sum

    val (updatedTokensPortfolio: Map[(HexAddress, TokenId), LeaderFollowingEntry], newLockedBalance: BigDecimal) =
      tokensToBeRemoved.foldLeft((tokens, lockedBalance)) {
        case ((tokens0, lockedBalance), (makerAddress, tokenId, entry, _)) =>
          val newLockedBalance = lockedBalance - entry.allowedTotalPrice + entry.ourTotalPricePutIn
          val newTokens        = tokens0.removed(makerAddress -> tokenId)
          (newTokens, newLockedBalance)
      }

    val unlockedBalance = lockedBalance - newLockedBalance
    self.copy(
      lockedBalance = newLockedBalance,
      freeBalance   = freeBalance + profit + unlockedBalance,
      tokens        = updatedTokensPortfolio,
    )
  }
}

object Wallet {
  final val PrimaryWalletId = "PRIMARY WALLET ID"

  def initWith(balance: BigDecimal, activeFromBlock: Int, activeToBlock: Option[Int], id: String): Wallet =
    Wallet(
      id              = id,
      initialBalance  = balance,
      lockedBalance   = BigDecimal(0),
      freeBalance     = balance,
      tokens          = Map.empty,
      activeFromBlock = activeFromBlock,
      activeToBlock   = activeToBlock,
    )

  def genWithRandomExpiration[F[_]: Sync](activeFromBlock: Int)(config: SimulationConfig): F[Wallet] = {
    for {
      random <- Random.scalaUtilRandom[F]
      activeToBlock <- random.betweenInt(
        activeFromBlock + config.minWalletBlocksLifetime,
        activeFromBlock + config.maxWalletBlocksLifetime
      )
      id <- UUIDGen.randomUUID[F]
    } yield initWith(config.initialWalletBalance, activeFromBlock, Some(activeToBlock), id.toString)
  }
}
