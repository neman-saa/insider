package org.github.insider.simulations

import cats.data.NonEmptyList
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry, LeaderboardStrategy}
import org.github.insider.polymarket.domain.Side
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class Simulator[F[_]: Async](
  leaderboard: LeaderboardStrategy[F, AdvancedLeaderboardEntry],
  simulationsRepository: SimulationsRepository[F],
)(
  leaderboardRef: Ref[F, Map[HexAddress, AdvancedLeaderboardEntry]],
  lastLeaderboardUpdateRef: Ref[F, Int],
  currentTimeRef: Ref[F, Instant],
  tokenInfosRef: Ref[F, Map[TokenId, TokenInfo]],
)(logger: Logger[F]) {

  def start(start: Int, end: Int)(config: SimulationConfig): F[Unit] = {
    for {
      currentTime <- simulationsRepository.timestampByBlockNum(start)
      _           <- currentTimeRef.set(currentTime)
      _           <- logger.info("Started load leaderboard")
      leaderboard <- leaderboard.load(start, limit = config.leaderboardLimit)
      _           <- logger.info(s"leaderboard length = ${leaderboard.size}")
      _           <- leaderboardRef.set(leaderboard)
      _           <- lastLeaderboardUpdateRef.set(start)

      primaryWallet   = Wallet.initWith(config.initialWalletBalance, start, None, Wallet.PrimaryWalletId)
      walletsPoolRef <- Ref.of[F, WalletsPool](WalletsPool.initWithPrimary(primaryWallet))

      ranges                = (start to end).grouped(config.blocksProcessingBatchSize).toList
      _                    <- ranges.traverse_(range => processRange(range.start, range.end)(walletsPoolRef, config))
      tokenInfos           <- tokenInfosRef.get
      updatedPrimaryWallet <- walletsPoolRef.get.map(_.primary)
      _ <- simulationsRepository.insertWallets(
        NonEmptyList.of(updatedPrimaryWallet.prepareForPersist(tokenInfos.view.mapValues(_.price).toMap))
      )
    } yield ()
  }

  private def processRange(from: Int, to: Int)(
    walletsPoolRef: Ref[F, WalletsPool],
    config: SimulationConfig,
  ): F[Unit] =
    for {
      _                        <- logger.info(s"Starting processing of [$from, $to] range")
      trades                   <- simulationsRepository.getHistoricalTrades(from, to)
      tradesNormalized          = trades.map(trade => trade.copy(amount = trade.amount / 1000000))
      maybeLatestBlockTimestamp = getLatestBlockTimestampFrom(trades)
      _                        <- logger.info(s"Trades fetched: ${trades.size}")
      currentTime              <- currentTimeRef.get
      leaderboard              <- leaderboardRef.get
      walletsPool              <- walletsPoolRef.get
      tokenInfos               <- tokenInfosRef.get
      filteredTrades            = tradesNormalized.filter(trade => leaderboard.contains(HexAddress(trade.makerAddress)))
      updatedTokenInfos         = updateTokenInfos(filteredTrades, tokenInfos, leaderboard)
      topTokens                 = findTopTokens(updatedTokenInfos, maybeLatestBlockTimestamp.getOrElse(currentTime))

      walletsNel = walletsPool.toNel
      processedWalletsNel = walletsNel.map(wallet =>
        wallet.updatePositions(topTokens, updatedTokenInfos.view.mapValues(_.price).toMap)
      )
      updatedWalletsPool = WalletsPool.fromNel(processedWalletsNel)
      (cleanedTokenInfos, expiredResolutions) =
        removeExpiredTokens(updatedTokenInfos, maybeLatestBlockTimestamp)

      resolvedWalletsPool = updatedWalletsPool.resolveTokens(expiredResolutions.view.mapValues(_.lastPrice).toMap)
      _                  <- tokenInfosRef.set(cleanedTokenInfos)
      _                  <- logger.info(s"Token Resolutions map size: ${cleanedTokenInfos.size}")

      (cleanedWalletsPool, expiredWallets) =
        resolvedWalletsPool.removeExpiredWallets(
          cleanedTokenInfos.view.mapValues(_.price).toMap,
          currentBlock                      = to,
          sellActiveTokensAtResolutionPrice = true
        )
      _ <- NonEmptyList.fromList(expiredWallets).fold(Async[F].unit)(simulationsRepository.insertWallets)

      newWallets     <- generateWallets(expiredWallets, cleanedWalletsPool.temporary.size, currentBlock = to)(config)
      nextWalletsPool = cleanedWalletsPool.addWallets(newWallets)
      _              <- walletsPoolRef.set(nextWalletsPool)
      _              <- logger.info(s"Temporary wallets in pool: + ${nextWalletsPool.temporary.size}")

      _ <- maybeReloadLeaderboard(currentBlock = to)(config)
      _ <- currentTimeRef.set(maybeLatestBlockTimestamp.getOrElse(currentTime))
      _ <- logger.info(
        s"Current balance: ${processedWalletsNel.head.prepareForPersist(updatedTokenInfos.view.mapValues(_.price).toMap).currentBalance}"
        // Если будешь смотреть, то тут очень странная херь, буквально за тыщу блоков с 1300 до 200 падает баланс и потом экспоненциально падает
      )
      _ <- logger.info(
        s"Current tokens sum: ${processedWalletsNel.head.positions.map(_.size).sum}"
      )
    } yield ()

  private def getLatestBlockTimestampFrom(trades: List[SimulationTrade]): Option[Instant] = {
    trades
      .reverse
      .collectFirst {
        case trade if trade.blockTimestamp.nonEmpty => trade.blockTimestamp
      }
      .flatten
  }

  private def updateTokenInfos(
    trades: List[SimulationTrade],
    tokenInfos: Map[TokenId, TokenInfo],
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry]
  ): Map[TokenId, TokenInfo] = {
    val newTokensInfos = trades.foldLeft(tokenInfos) {
      case (tokenScores, trade) =>
        val entry = leaderboard(HexAddress(trade.makerAddress))
        val score = trade.totalPrice /
          entry.avgBuy *
          entry.score /
          entry.totalLeaderboardScore *
          entry.totalLeaderboardSize * (trade.side match {
            case Buy  => 1
            case Sell => -1
          })
        val tokenScore =
          tokenScores
            .get(
              trade.tokenId
            )
            .map(_.score)
        val tokenInfo =
          TokenInfo(trade.totalPrice / trade.amount, trade.lastPrice, trade.closedTime, tokenScore.getOrElse(0))
        val oppositeTokenInfo =
          tokenScores.getOrElse(
            trade.oppositeTokenId,
            TokenInfo(1 - trade.totalPrice / trade.amount, (trade.lastPrice - 1).abs, trade.closedTime, 0)
          )
        tokenScores +
          (trade.tokenId         -> tokenInfo.copy(score = tokenInfo.score + score)) +
          (trade.oppositeTokenId -> oppositeTokenInfo.copy(score = oppositeTokenInfo.score - score))
    }
    newTokensInfos
  }

  private def removeExpiredTokens(
    tokenResolutions: Map[TokenId, TokenInfo],
    maybeLatestBlockTimestamp: Option[Instant],
  ): (Map[TokenId, TokenInfo], Map[TokenId, TokenInfo]) = {
    maybeLatestBlockTimestamp match {
      case Some(latestBlockTimestamp) =>
        tokenResolutions.partition {
          case (_, resolutionInfo) =>
            latestBlockTimestamp isBefore resolutionInfo.resolveDate
        }
      case None =>
        (tokenResolutions, Map.empty)
    }
  }

  private def findTopTokens(
    tokenInfos: Map[TokenId, TokenInfo],
    currentTime: Instant
  ): Map[TokenId, BigDecimal] =
    tokenInfos
      .map {
        case (tokenId, tokenInfo) =>
          val timeToResolve =
            (tokenInfo.resolveDate.getEpochSecond - currentTime.getEpochSecond).max(1)

          val efficiency =
            (1 - tokenInfo.price) * tokenInfo.score / timeToResolve

          tokenId -> (tokenInfo.price, efficiency)
      }
      .toList
      .filter(_._2._2 > 0)
      .sortBy(-_._2._2)
      .take(10)
      .map { case (tokenId, (price, _)) => tokenId -> price }
      .toMap

  private def generateWallets(expiredWallets: List[Wallet], activeWalletsCount: Int, currentBlock: Int)(
    config: SimulationConfig
  ): F[List[Wallet]] = {
    val fromBlocks         = expiredWallets.flatMap(wallet => wallet.activeToBlock.map(activeTo => activeTo + 1))
    val extendedFromBlocks = fromBlocks.padTo(config.maxTemporaryWalletsInPool - activeWalletsCount, currentBlock + 1)

    extendedFromBlocks.traverse { activeFromBlock =>
      Wallet.genWithRandomExpiration[F](activeFromBlock)(config)
    }
  }

  private def maybeReloadLeaderboard(currentBlock: Int)(config: SimulationConfig): F[Unit] =
    lastLeaderboardUpdateRef.get.flatMap { lastLeaderboardBlock =>
      if (currentBlock - lastLeaderboardBlock >= config.leaderboardBlocksLifetime) {
        for {
          leaderboard <- leaderboard.load(currentBlock, limit = config.leaderboardLimit)
          _           <- leaderboardRef.set(leaderboard)
          _           <- lastLeaderboardUpdateRef.set(currentBlock)
        } yield ()
      } else Async[F].unit
    }

}

object Simulator {
  def of[F[_]: Async](
    leaderboard: LeaderboardStrategy[F, AdvancedLeaderboardEntry],
    walletsRepository: SimulationsRepository[F],
  ): F[Simulator[F]] =
    for {
      logger                  <- Slf4jLogger.create[F]
      leaderboardRef          <- Ref.of[F, Map[HexAddress, AdvancedLeaderboardEntry]](Map.empty)
      lastLeaderboardBlockRef <- Ref.of[F, Int](-1)
      tokenResolutionsRef     <- Ref.of[F, Map[TokenId, TokenInfo]](Map.empty)
      currentTimeRef          <- Ref.of[F, Instant](Instant.now)
    } yield new Simulator[F](leaderboard, walletsRepository)(
      leaderboardRef,
      lastLeaderboardBlockRef,
      currentTimeRef,
      tokenResolutionsRef
    )(logger)
}
