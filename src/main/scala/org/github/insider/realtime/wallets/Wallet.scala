package org.github.insider.realtime.wallets

import cats.effect.{Async, Clock}
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.realtime.tokens.{TokenInfo, TokensInfoRegistry}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.FiniteDuration

final class Wallet[F[_]: Async] private (
  tokensInfoRegistry: TokensInfoRegistry[F],
  logger: Logger[F],
  tradingClient: TradingClient[F],
  marketsAmount: Int,
  thresholdPercent: Int
) {

  def updateEvery(duration: FiniteDuration): F[Unit] =
    performOperations().handleErrorWith(e => logger.error(e)(s"Failed to update wallet: ${e.getMessage}")) >>
      Clock[F].sleep(duration) >>
      updateEvery(duration)

  def performOperations(): F[Unit] = {

    final case class TokenPriceEfficiency(price: BigDecimal, efficiency: BigDecimal)
    final case class Holding(asset: String, size: BigDecimal, price: BigDecimal, efficiency: BigDecimal) {
      def totalPrice: BigDecimal = size * price
    }

    def isBetterMoreThanThreshold(candidateEfficiency: BigDecimal, currentEfficiency: BigDecimal): Boolean =
      candidateEfficiency > currentEfficiency * (BigDecimal(100 + thresholdPercent) / 100)

    def toHolding(position: Position, tokenInfos: Map[String, TokenPriceEfficiency]): Holding = {
      val info = tokenInfos
        .getOrElse(position.asset, TokenPriceEfficiency(0, -1))
      Holding(position.asset, position.size, info.price, info.efficiency)
    }
    def trySell(holding: Holding, shares: BigDecimal): F[Unit] =
      if (shares <= 0) Async[F].unit
      else
        tradingClient
          .sell(holding.asset, shares, None)
          .void
          .handleErrorWith(error =>
            logger.warn(error)(s"Could not sell ${holding.asset} shares=$shares, operation skipped")
          )

    def tryBuy(asset: String, money: BigDecimal): F[Unit] = {
      tradingClient.balance().flatMap { currentBalance =>
        val spend = money min currentBalance
        if (spend < 1) ().pure[F]
        else
          tradingClient
            .buy(asset, spend, None)
            .void
            .handleErrorWith(error => logger.warn(error)(s"Could not buy $asset for money=$spend, operation skipped"))
      }
    }

    def sellOverweight(holding: Holding, maxMarketPrice: BigDecimal): F[Unit] = {
      val excessPrice = holding.totalPrice - maxMarketPrice
      val shares      = if (holding.price > 0) excessPrice / holding.price else BigDecimal(0)

      trySell(holding, shares)
    }

    def currentHoldingsF(): F[List[Holding]] = {
      for {
        positions <- tradingClient.positions()
        infos <-
          if (positions.isEmpty) Map.empty[String, (BigDecimal, BigDecimal)].pure[F]
          else tokensInfoRegistry.tokensInfoForTokens(positions.map(_.asset))

        tokensPE = infos.map { case (id, (price, efficiency)) => id -> TokenPriceEfficiency(price, efficiency) }
      } yield positions.map(position => toHolding(position, tokensPE))
    }

    def totalBalance(currentBalance: BigDecimal, holdings: List[Holding]): BigDecimal =
      currentBalance + holdings.map(_.totalPrice).sum
    for {
      topInfos        <- tokensInfoRegistry.topTokensInfo
      currentHoldings <- currentHoldingsF().map(_.sortBy(holding => -holding.efficiency))
      topTokens = topInfos.map { case (id, (price, efficiency)) => id -> TokenPriceEfficiency(price, efficiency) }
      ourTopMarkets =
        currentHoldings
          .filter(_.efficiency > 0)
          .take(marketsAmount)

      ourRedundantMarkets =
        currentHoldings.filter(holding => !ourTopMarkets.map(_.asset).contains(holding.asset))

      topTokensIds = topTokens.map(_._1)

      (ourFromTopInfos, maybeOurToChange) =
        ourTopMarkets.partition(holding => topTokensIds.contains(holding.asset))

      topInfoCandidates =
        topTokens.filterNot { case (asset, _) => ourTopMarkets.exists(_.asset == asset) }

      (changedMarkets, unchangedMarkets) =
        topInfoCandidates
          .foldLeft((ourFromTopInfos, maybeOurToChange.sortBy(_.efficiency))) {
            case ((keptMarkets, remainingToChange), (candidateAsset, candidateInfo)) =>
              remainingToChange match {
                case worst :: rest if isBetterMoreThanThreshold(candidateInfo.efficiency, worst.efficiency) =>
                  (Holding(candidateAsset, 0, candidateInfo.price, candidateInfo.efficiency) :: keptMarkets, rest)
                case Nil if keptMarkets.size < marketsAmount =>
                  (Holding(candidateAsset, 0, candidateInfo.price, candidateInfo.efficiency) :: keptMarkets, Nil)
                case _ =>
                  (keptMarkets, remainingToChange)
              }
          }

      targetMarkets =
        (changedMarkets ++ unchangedMarkets)
          .sortBy(holding => -holding.efficiency)
          .take(marketsAmount)

      targetMarketIds = targetMarkets.map(_.asset).toSet

      positionsToSell =
        ourRedundantMarkets ++ maybeOurToChange.filterNot(holding => targetMarketIds.contains(holding.asset))

      _                          <- positionsToSell.traverse_(holding => trySell(holding, holding.size))
      holdingsAfterUnneededSells <- currentHoldingsF()
      balanceAfterUnneededSells  <- tradingClient.balance()
      overweightLimit = totalBalance(balanceAfterUnneededSells, holdingsAfterUnneededSells) / marketsAmount * 2
      _ <- holdingsAfterUnneededSells
        .filter(holding => targetMarketIds.contains(holding.asset) && holding.totalPrice > overweightLimit)
        .traverse_(holding => sellOverweight(holding, overweightLimit))

      holdingsAfterOverweightSells <- currentHoldingsF()
      balanceAfterOverweightSells  <- tradingClient.balance()
      targetMarketPrice = totalBalance(balanceAfterOverweightSells, holdingsAfterOverweightSells) / marketsAmount
      missingMarkets = targetMarkets
        .filterNot(target => holdingsAfterOverweightSells.exists(_.asset == target.asset))
        .sortBy(holding => -holding.efficiency)
      _ <- missingMarkets.traverse_(target => tryBuy(target.asset, targetMarketPrice - target.totalPrice))
      holdingsAfterMissingBuys <- currentHoldingsF()
      missingMarketIds          = missingMarkets.map(_.asset).toSet
      underweightMarkets =
        holdingsAfterMissingBuys
          .filter(holding =>
            targetMarketIds.contains(holding.asset) &&
              !missingMarketIds.contains(holding.asset) &&
              holding.totalPrice < targetMarketPrice
          )
          .sortBy(holding => -holding.efficiency)

      _ <- underweightMarkets.traverse_(target => tryBuy(target.asset, targetMarketPrice - target.totalPrice))
    } yield ()
  }
}

object Wallet {
  def of[F[_]: Async](
    registry: TokensInfoRegistry[F],
    tradingClient: TradingClient[F],
    marketsAmount: Int,
    thresholdPercent: Int
  ): F[Wallet[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new Wallet[F](registry, logger, tradingClient, marketsAmount, thresholdPercent))

}
