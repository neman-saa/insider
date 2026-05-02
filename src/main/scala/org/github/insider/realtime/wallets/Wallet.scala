package org.github.insider.realtime.wallets

import cats.effect.{Async, Clock}
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.realtime.tokens.{TokenInfo, TokenInfoShort, TokensInfoRegistry}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
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

    final case class Holding(
      asset: String,
      size: BigDecimal,
      price: BigDecimal,
      efficiency: BigDecimal,
      buyTime: Option[Instant]
    ) {
      def totalPrice: BigDecimal = size * price
    }

    def isNew(holding: Holding, time: Instant): Boolean =
      holding.buyTime.forall(time.getEpochSecond - _.getEpochSecond > 1800)

    def isBetterMoreThanThreshold(candidateEfficiency: BigDecimal, currentEfficiency: BigDecimal): Boolean =
      candidateEfficiency > currentEfficiency * (BigDecimal(100 + thresholdPercent) / 100)

    def toHolding(position: Position, tokenInfos: Map[String, TokenInfoShort]): Holding = {
      val info = tokenInfos
        .getOrElse(position.asset, TokenInfoShort(position.asset, -1, None, 0))
      Holding(position.asset, position.size, info.price, info.efficiency, info.buyTime)
    }
    def trySell(asset: String, shares: BigDecimal): F[Unit] =
      if (shares <= 0) Async[F].unit
      else
        tradingClient
          .sell(asset, shares, None)
          .void
          .handleErrorWith(error =>
            logger.warn(error)(s"Could not sell $asset shares=$shares, operation skipped")
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

      trySell(holding.asset, shares)
    }

    def currentHoldingsF(): F[List[Holding]] = {
      for {
        positions <- tradingClient.positions()
        infos <-
          if (positions.isEmpty) Map.empty[String, TokenInfoShort].pure[F]
          else tokensInfoRegistry.tokensInfoForTokens(positions.map(_.asset))

      } yield positions.map(position => toHolding(position, infos))
    }

    def totalBalance(currentBalance: BigDecimal, holdings: List[Holding]): BigDecimal =
      currentBalance + holdings.map(_.totalPrice).sum

    for {
      now             <- Clock[F].realTimeInstant
      topInfos        <- tokensInfoRegistry.topTokensInfo
      currentHoldings <- currentHoldingsF().map(_.sortBy(holding => -holding.efficiency))

      (ourPositive, _) =
        currentHoldings
          .partition(_.efficiency > 0)

      (ourNew, ourOld) = ourPositive.partition(now.getEpochSecond - _.buyTime.getOrElse(now).getEpochSecond < 1800)

      ourTopMarkets = ourNew ++ ourOld.sortBy(-_.efficiency).take(marketsAmount - ourNew.length)

      ourRedundantMarkets =
        currentHoldings.filter(holding => !ourTopMarkets.map(_.asset).contains(holding.asset))

      topTokensIds = topInfos.map(_._1)

      (_, maybeOurToChange) =
        ourTopMarkets.partition(holding => topTokensIds.contains(holding.asset))

      topInfoCandidates =
        topInfos.filterNot { case (asset, _) => ourTopMarkets.exists(_.asset == asset) }

      (toBuyMarkets, toSellMarkets, _, updateEffect) =
        topInfoCandidates
          .foldLeft((List.empty[TokenInfoShort], List.empty[Holding], maybeOurToChange.sortBy(_.efficiency), ().pure[F])) {

            case ((toBuyMarkets, toSellMarkets, remainingToChange, updateEffect), (candidateAsset, candidateInfo)) =>
              remainingToChange match {

                case worst :: rest
                    if isBetterMoreThanThreshold(candidateInfo.efficiency, worst.efficiency) &&
                      !isNew(worst, now) =>
                  (
                    candidateInfo :: toBuyMarkets,
                    worst :: toSellMarkets,
                    rest,
                    updateEffect >> tokensInfoRegistry.setBuyTimeBuyPrice(candidateAsset, now, candidateInfo.price)
                  )

                case worst :: rest if isBetterMoreThanThreshold(candidateInfo.efficiency, worst.efficiency) =>
                  (
                    toBuyMarkets,
                    toSellMarkets,
                    rest,
                    updateEffect
                  )

                case Nil if topTokensIds.length + toBuyMarkets.length - toSellMarkets.length < marketsAmount =>
                  (
                    candidateInfo :: toBuyMarkets,
                    toSellMarkets,
                    Nil,
                    updateEffect >> tokensInfoRegistry.setBuyTimeBuyPrice(candidateAsset, now, candidateInfo.price)
                  )

                case _ =>
                  (toBuyMarkets, toSellMarkets, remainingToChange, updateEffect)
              }
          }

      positionsToSell =
        ourRedundantMarkets ++ toSellMarkets

      _                          <- updateEffect
      _                          <- positionsToSell.traverse_(holding => trySell(holding.asset, holding.size))

      holdingsAfterUnneededSells <- currentHoldingsF()
      balanceAfterUnneededSells  <- tradingClient.balance()
      overweightLimit = totalBalance(balanceAfterUnneededSells, holdingsAfterUnneededSells) / marketsAmount * 2

      _ <- holdingsAfterUnneededSells
        .filter(holding => !positionsToSell.map(_.asset).contains(holding.asset) && holding.totalPrice > overweightLimit)
        .traverse_(holding => sellOverweight(holding, overweightLimit))

      holdingsAfterOverweightSells <- currentHoldingsF()
      balanceAfterOverweightSells  <- tradingClient.balance()
      targetMarketPrice = totalBalance(balanceAfterOverweightSells, holdingsAfterOverweightSells) / marketsAmount

      _ <- toBuyMarkets.sortBy(-_.efficiency).traverse_(target => tryBuy(target.id, targetMarketPrice))

      holdingsAfterMissingBuys <- currentHoldingsF()

      underweightMarkets =
        holdingsAfterMissingBuys
          .filter(holding =>
            !toBuyMarkets.contains(holding.asset) &&
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
