package org.github.insider.simulations

import cats.effect.Sync
import cats.effect.std.{Random, UUIDGen}
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.github.insider.polymarket.domain.Side.{Buy, Sell}

import scala.math._

final case class Wallet(
  id: String,
  initialBalance: BigDecimal,
  currentBalance: BigDecimal,
  positions: List[(Position, BigDecimal)],
  activeFromBlock: Int,
  activeToBlock: Option[Int],
  operations: List[Operation],
  currentBlock: Long
) { self =>

  /** Returns updated wallet if buy succeeds, otherwise returns None */
  def updatePositions(
    topAssets: Map[TokenId, BigDecimal],
    /** Tokens that should be bought by the given price */
    assetsInfos: Map[TokenId, BigDecimal],
    newBlock: Long
    /**
      * Info about all tokens that are not resolved, needed to sell not important tokens. Always will have info about
      * wallet positions
      */
  ): Wallet = {
    val (toBeRemoved, toBeRemained) = positions.partition(pos => !topAssets.contains(pos._1.asset))

    if (toBeRemained.map(_._1.asset).toSet == topAssets.keySet && toBeRemoved.isEmpty) self
    else {

      val newBalance = currentBalance + toBeRemoved.foldLeft(BigDecimal(0))((balance, position) =>
        balance + position._1.size * assetsInfos(position._1.asset)
      )

      val buyPerToken = newBalance / (20 - toBeRemained.length)
      val toBeBought  = topAssets -- toBeRemained.map(_._1.asset)

      val newPositions = toBeBought.map {
        case (asset, tokenPrice) => (Position(asset, buyPerToken / tokenPrice), tokenPrice)
      }

      val buyCost  = toBeBought.size * buyPerToken
      val remained = newBalance - buyCost

      val sellOperations = toBeRemoved.map {
        case (Position(asset, size), _) => Operation(id, asset, Sell, size, size * assetsInfos(asset), newBlock)
      }
      val buyOperations = newPositions.map {
        case (Position(asset, size), _) => Operation(id, asset, Buy, size, buyPerToken, newBlock)
      }

      self.copy(
        positions      = toBeRemained ++ newPositions,
        currentBalance = remained,
        operations     = operations ++ buyOperations ++ sellOperations,
        currentBlock   = newBlock
      )
    }
  }

  /** Returns updated wallet if sell succeeds, otherwise returns None */

  def resolveTokens(tokenLastPrices: Map[TokenId, BigDecimal]): Wallet = {
    val (tokensToBeRemoved, remainingPositions) =
      positions.partition(position => tokenLastPrices.contains(position._1.asset))
    val addToBalance: BigDecimal = tokensToBeRemoved.map {
      case (Position(asset, size), _) =>
        size * tokenLastPrices(asset) // already normalized ???
    }.sum

    val sellOperations = tokensToBeRemoved.map {
      case (Position(asset, size), _) => Operation(id, asset, Sell, size, size * tokenLastPrices(asset), currentBlock)
    }

    self.copy(
      positions      = remainingPositions,
      currentBalance = currentBalance + addToBalance,
      operations     = operations ++ sellOperations
    )
  }

  def prepareForPersist(tokenCurrentPrices: Map[TokenId, BigDecimal]): Wallet = {
    val balance = positions.foldLeft(BigDecimal(0)) {
      case (balance, position) =>
        balance + position._1.size * tokenCurrentPrices(position._1.asset)
    }

    val sellOperations = positions.map {
      case (Position(asset, size), _) =>
        Operation(id, asset, Sell, size, size * tokenCurrentPrices(asset), currentBlock)
    }
    self.copy(
      currentBalance = currentBalance + balance,
      positions      = Nil,
      operations     = operations ++ sellOperations
    )
  }

}

object Wallet {
  final val PrimaryWalletId = "PRIMARY WALLET ID"

  def initWith(
    balance: BigDecimal,
    activeFromBlock: Int,
    activeToBlock: Option[Int],
    id: String,
  ): Wallet =
    Wallet(
      id              = id,
      initialBalance  = balance,
      currentBalance  = balance,
      positions       = Nil,
      activeFromBlock = activeFromBlock,
      activeToBlock   = activeToBlock,
      operations      = Nil,
      currentBlock    = activeFromBlock
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
