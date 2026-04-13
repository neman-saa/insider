package org.github.insider.simulations

import cats.effect.Sync
import cats.effect.std.{Random, UUIDGen}
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import scala.math._

final case class Wallet(
  id: String,
  initialBalance: BigDecimal,
  currentBalance: BigDecimal,
  positions: List[Position],
  activeFromBlock: Int,
  activeToBlock: Option[Int],
) { self =>

  /** Returns updated wallet if buy succeeds, otherwise returns None */
  def updatePositions(
    topAssets: Map[TokenId, BigDecimal],
    /** Tokens that should be bought by the given price */
    assetsInfos: Map[TokenId, BigDecimal]
    /**
      * Info about all tokens that are not resolved, needed to sell not important tokens. Always will have info about
      * wallet positions
      */
  ): Wallet = {
    val (toBeRemoved, toBeRemained) = positions.partition(pos => !topAssets.contains(pos.asset))
    val newBalance = currentBalance + toBeRemoved.foldLeft(BigDecimal(0))((balance, position) =>
      balance + position.size * assetsInfos(position.asset)
    )

    val toBeBought = assetsInfos -- positions.map(_.asset)
    val newPositions = toBeBought.map {
      case (asset, tokenPrice) => Position(asset, newBalance / toBeBought.size / tokenPrice)
    }
    self.copy(
      positions      = toBeRemained ++ newPositions,
      currentBalance = 0
    )
  }

  /** Returns updated wallet if sell succeeds, otherwise returns None */

  def resolveTokens(tokenLastPrices: Map[TokenId, BigDecimal]): Wallet = {
    val (tokensToBeRemoved, remainingPositions) =
      positions.partition(position => tokenLastPrices.contains(position.asset))
    val addToBalance: BigDecimal = tokensToBeRemoved.map {
      case Position(asset, size) =>
        size * tokenLastPrices(asset) // already normalized ???
    }.sum

    self.copy(
      positions      = remainingPositions,
      currentBalance = currentBalance + addToBalance
    )
  }

  def prepareForPersist(tokenCurrentPrices: Map[TokenId, BigDecimal]): Wallet = {
    val balance = positions.foldLeft(BigDecimal(0)) {
      case (balance, position) =>
        balance + position.size*tokenCurrentPrices(position.asset)
    }

    self.copy(
      currentBalance = currentBalance + balance,
      positions      = Nil
    )
  }

}

object Wallet {
  final val PrimaryWalletId = "PRIMARY WALLET ID"

  def initWith(
    balance: BigDecimal,
    activeFromBlock: Int,
    activeToBlock: Option[Int],
    id: String
  ): Wallet =
    Wallet(
      id              = id,
      initialBalance  = balance,
      currentBalance  = balance,
      positions       = Nil,
      activeFromBlock = activeFromBlock,
      activeToBlock   = activeToBlock,
    )

  def genWithRandomExpiration[F[_]: Sync](activeFromBlock: Int)(
    config: SimulationConfig
  ): F[Wallet] = {
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
