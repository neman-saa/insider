package org.github.insider.simulations

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.effect.std.{Random, UUIDGen}
import cats.syntax.all._
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry}
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.github.insider.simulations.LeaderFollowingEntry.TokenOperation

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
    blockNum: Int,
    extraBuyPerCents: Int,
    allowedPerCentsPerUser: Int
  ): Option[Wallet] = {
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
            extraBuyPerCents / 100
        val allowedPrice = freeBalance * allowedPerCentsPerUser / 100

        val ourFirstPricePutIn = ourFirstPrice min allowedPrice
        val leaderFollowingEntry = LeaderFollowingEntry(
          leader,
          totalPrice,
          ourFirstPrice,
          ourFirstPrice,
          allowedPrice,
          ourFirstPricePutIn * singleTokenPrice
        )
        val newTokens        = tokens + ((leader, tokenId) -> leaderFollowingEntry)
        val newFreeBalance   = freeBalance - freeBalance * allowedPerCentsPerUser / 100
        val newLockedBalance = lockedBalance + freeBalance * (allowedPerCentsPerUser - extraBuyPerCents) / 100
        Some(this.copy(tokens = newTokens, freeBalance = newFreeBalance, lockedBalance = newLockedBalance))

      case Some(followingEntry) =>
        val ourNewBuy          = followingEntry.allowedTotalPrice * followingEntry.leaderFirstBuy / totalPrice
        val singleTokenPrice   = totalPrice / amount + 0.01
        val ourTotalPrice      = followingEntry.ourTotalPrice + ourNewBuy
        val ourTotalPricePutIn = ourTotalPrice min followingEntry.allowedTotalPrice
        val ourAmount          = (ourTotalPricePutIn - followingEntry.ourTotalPricePutIn) * singleTokenPrice
        val leaderOperation    = TokenOperation(Buy, amount, totalPrice / amount, blockNum)
        val newEntry = LeaderFollowingEntry(
          leader,
          followingEntry.leaderFirstBuy,
          ourTotalPrice,
          ourTotalPricePutIn,
          followingEntry.allowedTotalPrice,
          followingEntry.ourAmount + ourAmount
        )
        val newLockedBalance = lockedBalance - (ourTotalPricePutIn - followingEntry.ourTotalPricePutIn)
        Some(this.copy(tokens = tokens + ((leader, tokenId) -> newEntry), lockedBalance = newLockedBalance))
    }
  }

  /** Returns updated wallet if sell succeeds, otherwise returns None */
  def copySell(
    tokenId: String,
    leader: HexAddress,
    amount: BigDecimal,
    totalPrice: BigDecimal,
    blockNum: Int,
  ): Option[Wallet] = {
    tokens.get((leader, tokenId)) match {
      case None => None
      case Some(followingEntry) =>
        val singleTokenPrice = totalPrice / amount - 0.01
        val ourTotalPrice =
          followingEntry.ourTotalPrice - followingEntry.leaderFirstBuy / totalPrice * followingEntry.allowedTotalPrice
        val ourTotalPricePutIn = ourTotalPrice min followingEntry.allowedTotalPrice
        val ourAmount          = (ourTotalPricePutIn - ourTotalPrice) * singleTokenPrice
        val newLockedBalance   = lockedBalance + ourTotalPricePutIn - ourTotalPrice
        val entry = LeaderFollowingEntry(
          leader,
          followingEntry.leaderFirstBuy,
          ourTotalPrice,
          ourTotalPricePutIn,
          followingEntry.allowedTotalPrice,
          followingEntry.ourAmount - ourAmount
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

    val updatedTokensPortfolio: Map[(HexAddress, TokenId), LeaderFollowingEntry] =
      tokensToBeRemoved.foldLeft(tokens) {
        case (tokens0, (makerAddress, tokenId, _, _)) =>
          tokens0.removed(makerAddress -> tokenId)
      }

    self.copy(
      lockedBalance = if (lockProfit) self.lockedBalance + profit else self.lockedBalance,
      freeBalance   = if (!lockProfit) self.freeBalance + profit else self.freeBalance,
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
