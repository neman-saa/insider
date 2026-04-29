package org.github.insider.realtime.wallets

import cats.effect.Async
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.realtime.tokens.TokensInfoRegistry
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class Wallet[F[_]: Async] private (
  tokensInfoRegistry: TokensInfoRegistry[F],
  logger: Logger[F],
  tradingClient: TradingClient[F],
  marketsAmount: Int,
  thresholdPercent: Int,
  spreadPercent: Int
) {

  def updateWallet(): F[Unit] = for {
    infos     <- tokensInfoRegistry.tokensInfo
    positions <- tradingClient.positions()
    _         <- performOperations(infos, positions)
  } yield ()

  private def performOperations(
    infos: Map[String, (BigDecimal, BigDecimal)],
    positions: List[Position]
  ): F[Unit] = {
    final case class TokenInfo(price: BigDecimal, efficiency: BigDecimal)
    final case class Holding(asset: String, size: BigDecimal, price: BigDecimal, efficiency: BigDecimal) {
      def totalPrice: BigDecimal = size * price
    }

    def isBetterMoreThanThreshold(candidateEfficiency: BigDecimal, currentEfficiency: BigDecimal): Boolean =
      candidateEfficiency > currentEfficiency * (BigDecimal(100 + thresholdPercent) / 100)

    def maxPrice(price: BigDecimal): BigDecimal =
      price + BigDecimal(spreadPercent) / 100

    def minPrice(price: BigDecimal): BigDecimal =
      price - BigDecimal(spreadPercent) / 100

    def toHolding(position: Position, tokenInfos: Map[String, TokenInfo]): Option[Holding] =
      tokenInfos
        .get(position.asset)
        .map(info => Holding(position.asset, position.size, info.price, info.efficiency))

    def trySell(holding: Holding, shares: BigDecimal): F[Unit] =
      if (shares <= 0) Async[F].unit
      else
        tradingClient
          .sell(holding.asset, shares, Option.when(holding.price > 0)(minPrice(holding.price)))
          .void
          .handleErrorWith(error =>
            logger.warn(error)(s"Could not sell ${holding.asset} shares=$shares, operation skipped")
          )

    def tryBuy(asset: String, info: TokenInfo, amountToSpend: BigDecimal): F[Unit] = {
      val priceWithSpread = maxPrice(info.price)

      if (amountToSpend <= 0 || priceWithSpread <= 0) Async[F].unit
      else
        tradingClient.balance().flatMap { currentBalance =>
          val spend  = amountToSpend.min(currentBalance)
          val shares = spend / priceWithSpread

          if (shares <= 0) Async[F].unit
          else
            tradingClient
              .buy(asset, shares, Some(priceWithSpread))
              .void
              .handleErrorWith(error =>
                logger.warn(error)(s"Could not buy $asset for amount=$spend, operation skipped")
              )
        }
    }

    def sellOverweight(holding: Holding, maxMarketPrice: BigDecimal): F[Unit] = {
      val excessPrice = holding.totalPrice - maxMarketPrice
      val shares      = if (holding.price > 0) excessPrice / holding.price else BigDecimal(0)

      trySell(holding, shares)
    }

    def currentHoldings(tokenInfos: Map[String, TokenInfo]): F[List[Holding]] =
      tradingClient.positions().map(_.flatMap(position => toHolding(position, tokenInfos)))

    def totalBalance(currentBalance: BigDecimal, holdings: List[Holding]): BigDecimal =
      currentBalance + holdings.map(_.totalPrice).sum

    def buyUntilTarget(
      holdings: List[Holding],
      asset: String,
      info: TokenInfo,
      targetMarketPrice: BigDecimal
    ): F[Unit] = {
      val currentPrice  = holdings.find(_.asset == asset).fold(BigDecimal(0))(_.totalPrice)
      val amountToSpend = targetMarketPrice - currentPrice

      tryBuy(asset, info, amountToSpend)
    }

    val tokenInfos = infos.view.mapValues { case (price, efficiency) => TokenInfo(price, efficiency) }.toMap

    val topInfos = infos
      .toList
      .filter {
        case (_, (_, efficiency)) => efficiency > 0
      }
      .sortBy {
        case (_, (_, efficiency)) => -efficiency
      }
      .take(marketsAmount)
      .map { case (asset, (price, efficiency)) => asset -> TokenInfo(price, efficiency) }

    val positionsWithEfficiency =
      positions
        .flatMap(position => toHolding(position, tokenInfos))

    val positionsSortedByEfficiency =
      positionsWithEfficiency
        .sortBy(holding => -holding.efficiency)

    val ourTopMarkets =
      positionsSortedByEfficiency
        .take(marketsAmount)

    val ourRedundantMarkets =
      positionsSortedByEfficiency
        .drop(marketsAmount)

    val topInfoIds = topInfos.map(_._1).toSet

    val (ourFromTopInfos, maybeOurToChange) =
      ourTopMarkets.partition(holding => topInfoIds.contains(holding.asset))

    val topInfoCandidates =
      topInfos.filterNot { case (asset, _) => ourTopMarkets.exists(_.asset == asset) }

    val (changedMarkets, unchangedMarkets) =
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

    val targetMarkets =
      (changedMarkets ++ unchangedMarkets)
        .sortBy(holding => -holding.efficiency)
        .take(marketsAmount)

    val targetMarketIds = targetMarkets.map(_.asset).toSet

    val positionsToSell =
      ourRedundantMarkets ++ maybeOurToChange.filterNot(holding => targetMarketIds.contains(holding.asset))

    for {
      _ <- positionsToSell.traverse_(holding => trySell(holding, holding.size))

      holdingsAfterUnneededSells <- currentHoldings(tokenInfos)
      balanceAfterUnneededSells  <- tradingClient.balance()
      overweightLimit = totalBalance(balanceAfterUnneededSells, holdingsAfterUnneededSells) / marketsAmount * 2
      _ <- holdingsAfterUnneededSells
        .filter(holding => targetMarketIds.contains(holding.asset) && holding.totalPrice > overweightLimit)
        .traverse_(holding => sellOverweight(holding, overweightLimit))

      holdingsAfterOverweightSells <- currentHoldings(tokenInfos)
      balanceAfterOverweightSells  <- tradingClient.balance()
      targetMarketPrice = totalBalance(balanceAfterOverweightSells, holdingsAfterOverweightSells) / marketsAmount
      missingMarkets = targetMarkets
        .filterNot(target => holdingsAfterOverweightSells.exists(_.asset == target.asset))
        .sortBy(holding => -holding.efficiency)
      _ <- missingMarkets.traverse_(target =>
        tokenInfos
          .get(target.asset)
          .fold(Async[F].unit)(info =>
            buyUntilTarget(holdingsAfterOverweightSells, target.asset, info, targetMarketPrice)
          )
      )
      holdingsAfterMissingBuys <- currentHoldings(tokenInfos)
      missingMarketIds = missingMarkets.map(_.asset).toSet
      underweightMarkets =
        holdingsAfterMissingBuys
          .filter(holding =>
            targetMarketIds.contains(holding.asset) &&
              !missingMarketIds.contains(holding.asset) &&
              holding.totalPrice < targetMarketPrice
          )
          .sortBy(holding => -holding.efficiency)

      _ <- underweightMarkets.traverse_(target =>
        tokenInfos
          .get(target.asset)
          .fold(Async[F].unit)(info => buyUntilTarget(holdingsAfterMissingBuys, target.asset, info, targetMarketPrice))
      )
    } yield ()
  }
}

object Wallet {
  def of[F[_]: Async](
    registry: TokensInfoRegistry[F],
    tradingClient: TradingClient[F],
    marketsAmount: Int,
    thresholdPercent: Int,
    spreadPercent: Int
  ): F[Wallet[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new Wallet[F](registry, logger, tradingClient, marketsAmount, thresholdPercent, spreadPercent))

}
