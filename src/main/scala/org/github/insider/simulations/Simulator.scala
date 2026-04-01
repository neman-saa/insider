package org.github.insider.simulations

import cats.data.NonEmptyList
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry, LeaderboardStrategy}
import org.github.insider.polymarket.domain.Side
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class Simulator[F[_]: Async](
  leaderboard: LeaderboardStrategy[F, AdvancedLeaderboardEntry],
  simulationsRepository: SimulationsRepository[F],
)(
  leaderboardRef: Ref[F, Map[HexAddress, AdvancedLeaderboardEntry]],
  lastLeaderboardBlockRef: Ref[F, Int],
  tokenResolutionsRef: Ref[F, Map[TokenId, TokenResolutionInfo]],
)(logger: Logger[F]) {

  def start(start: Int, end: Int)(config: SimulationConfig): F[Unit] = {
    for {
      leaderboard <- leaderboard.load(start, limit = config.leaderboardLimit)
      _           <- leaderboardRef.set(leaderboard)
      _           <- lastLeaderboardBlockRef.set(start)

      primaryWallet   = Wallet.initWith(config.initialWalletBalance, start, None, Wallet.PrimaryWalletId)
      walletsPoolRef <- Ref.of[F, WalletsPool](WalletsPool.initWithPrimary(primaryWallet))

      ranges = (start to end).grouped(config.blocksProcessingBatchSize).toList
      _     <- ranges.traverse_(range => processRange(range.start, range.end)(walletsPoolRef, config))

      updatedPrimaryWallet <- walletsPoolRef.get.map(_.primary)
      _                    <- simulationsRepository.insertWallets(NonEmptyList.of(updatedPrimaryWallet))
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

      leaderboard <- leaderboardRef.get
      walletsPool <- walletsPoolRef.get

      walletsNel          = walletsPool.toNel
      processedWalletsNel = walletsNel.map(wallet => processTrades(tradesNormalized, wallet, leaderboard)(config))

      updatedWalletsPool = WalletsPool.fromNel(processedWalletsNel)

      tokenResolutions <- tokenResolutionsRef.get
      updatedTokenResolutions = updateTokenResolutions(tradesNormalized, tokenResolutions)
      (cleanedTokenResolutions, expiredResolutions) =
        removeExpiredTokens(updatedTokenResolutions, maybeLatestBlockTimestamp)

      resolvedWalletsPool = updatedWalletsPool.resolveTokens(expiredResolutions)
      _                      <- tokenResolutionsRef.set(cleanedTokenResolutions)
      _                      <- logger.info(s"Token Resolutions map size: ${cleanedTokenResolutions.size}")

      (cleanedWalletsPool, expiredWallets) =
        resolvedWalletsPool.removeExpiredWallets(
          cleanedTokenResolutions,
          currentBlock                      = to,
          sellActiveTokensAtResolutionPrice = true
        )
      _ <- NonEmptyList.fromList(expiredWallets).fold(Async[F].unit)(simulationsRepository.insertWallets)

      newWallets     <- generateWallets(expiredWallets, cleanedWalletsPool.temporary.size, currentBlock = to)(config)
      nextWalletsPool = cleanedWalletsPool.addWallets(newWallets)
      _              <- walletsPoolRef.set(nextWalletsPool)
      _              <- logger.info(s"Temporary wallets in pool: + ${nextWalletsPool.temporary.size}")

      _ <- maybeReloadLeaderboard(currentBlock = to)(config)
    } yield ()

  private def getLatestBlockTimestampFrom(trades: List[SimulationTrade]): Option[Instant] = {
    trades
      .reverse
      .collectFirst {
        case trade if trade.blockTimestamp.nonEmpty => trade.blockTimestamp
      }
      .flatten
  }

  private def updateTokenResolutions(
    trades: List[SimulationTrade],
    tokenResolutions: Map[TokenId, TokenResolutionInfo]
  ): Map[TokenId, TokenResolutionInfo] = {
    val tokenResolutionsFromTrades =
      trades.map(trade => trade.tokenId -> TokenResolutionInfo(trade.lastPrice, trade.closedTime))

    tokenResolutions ++ tokenResolutionsFromTrades
  }

  private def removeExpiredTokens(
    tokenResolutions: Map[TokenId, TokenResolutionInfo],
    maybeLatestBlockTimestamp: Option[Instant],
  ): (Map[TokenId, TokenResolutionInfo], Map[TokenId, TokenResolutionInfo]) = {
    maybeLatestBlockTimestamp match {
      case Some(latestBlockTimestamp) =>
        tokenResolutions.partition {
          case (_, resolutionInfo)  =>
            latestBlockTimestamp isAfter resolutionInfo.resolveDate
        }
      case None =>
        (tokenResolutions, Map.empty)
    }
  }

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
    lastLeaderboardBlockRef.get.flatMap { lastLeaderboardBlock =>
      if (currentBlock - lastLeaderboardBlock >= config.leaderboardBlocksLifetime) {
        for {
          leaderboard <- leaderboard.load(currentBlock, limit = config.leaderboardLimit)
          _           <- leaderboardRef.set(leaderboard)
          _           <- lastLeaderboardBlockRef.set(currentBlock)
        } yield ()
      } else Async[F].unit
    }

  private def processTrades(
    trades: List[SimulationTrade],
    wallet: Wallet,
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry],
  )(config: SimulationConfig): Wallet = {
    val tradesMatchesLeaderboard: List[(SimulationTrade, AdvancedLeaderboardEntry)] =
      trades.flatMap(trade => leaderboard.get(HexAddress(trade.makerAddress)).map(entry => (trade, entry)))
    val updatedWallet: Wallet =
      tradesMatchesLeaderboard.foldLeft[Wallet](wallet) {
        case (currentWallet, (trade, leaderboardEntry)) =>
          trade.side match {
            case Side.Buy =>
              val maybeUpdatedWallet =
                currentWallet.copyBuy(
                  tokenId          = trade.tokenId,
                  leader           = HexAddress(trade.makerAddress),
                  amount           = trade.amount,
                  totalPrice       = trade.totalPrice,
                  leaderboardEntry = leaderboardEntry,
                )(config)
              maybeUpdatedWallet.getOrElse(currentWallet)
            case Side.Sell =>
              val maybeUpdatedWallet =
                currentWallet.copySell(
                  tokenId    = trade.tokenId,
                  leader     = HexAddress(trade.makerAddress),
                  amount     = trade.amount,
                  totalPrice = trade.totalPrice,
                )

              maybeUpdatedWallet.getOrElse(currentWallet)
          }
      }

    updatedWallet
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
      tokenResolutionsRef     <- Ref.of[F, Map[TokenId, TokenResolutionInfo]](Map.empty)
    } yield new Simulator[F](leaderboard, walletsRepository)(
      leaderboardRef,
      lastLeaderboardBlockRef,
      tokenResolutionsRef
    )(logger)
}
