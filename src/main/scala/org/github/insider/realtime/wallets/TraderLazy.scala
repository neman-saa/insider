package org.github.insider.realtime.wallets

import cats.data.NonEmptyList
import cats.effect.{Async, Clock}
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.polymarket.domain.Position
import org.github.insider.realtime.tokens.{TokenInfoShort, TokensInfoRegistry}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.notifications.services.InsiderTelegramBot
import org.github.insider.polymarket.domain.Side
import org.github.insider.polymarket.domain.Side._
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

class TraderLazy[F[_]: Async] private(
  tokensInfoRegistry: TokensInfoRegistry[F],
  logger: Logger[F],
  tradingClient: TradingClient[F],
  secondsBeforeResolve: Int,
  marketsAmount: Int,
  notificator: InsiderTelegramBot[F]
) extends Trader[F] {

  override def performOperations: F[Unit] = {

    final case class Holding(
      asset: String,
      size: BigDecimal,
      price: BigDecimal,
      efficiency: BigDecimal,
      buyTime: Option[Instant],
      resolveDate: Instant,
      score: BigDecimal
    ) {
      def totalPrice: BigDecimal = size * price
    }

    def createMessage(
      side: Side,
      id: String,
      price: BigDecimal,
      avgPrice: BigDecimal,
      amount: BigDecimal,
      score: BigDecimal,
      latestScore: BigDecimal
    ): String =
      s"""
         |$side token: $id
         |preferred price: $price
         |avg price: $avgPrice
         |amount: $amount
         |score: $score
         |latest score: $latestScore
         |""".stripMargin

    def isCloseToResolve(now: Instant, holding: Holding): Boolean =
      holding.resolveDate.getEpochSecond - secondsBeforeResolve < now.getEpochSecond

    def toHolding(position: Position, tokenInfos: Map[String, TokenInfoShort]): Holding = {
      val info = tokenInfos
        .getOrElse(position.asset, TokenInfoShort(position.asset, -1, None, 0, Instant.now, 0))
      Holding(position.asset, position.size, info.price, info.efficiency, info.buyTime, info.resolveDate, info.score)
    }
    def trySell(holding: Holding, shares: BigDecimal, latestScore: BigDecimal): F[Unit] =
      if (shares <= 0) Async[F].unit
      else
        tradingClient
          .sell(holding.asset, shares, None)
          .flatMap { result =>
            val message =
              createMessage(
                Buy,
                holding.asset,
                holding.price,
                result.totalPrice / result.amount,
                result.amount,
                holding.score,
                latestScore
              )
            notificator.sendTradeInfo(message)
          }
          .handleErrorWith(error =>
            logger.warn(error)(s"Could not sell ${holding.asset} shares=$shares, operation skipped")
          )

    def tryBuy(info: TokenInfoShort, money: BigDecimal, latestScore: BigDecimal): F[Unit] = {
      tradingClient.balance().flatMap { currentBalance =>
        val spend = money min currentBalance
        if (spend < 1) ().pure[F]
        else
          tradingClient
            .buy(info.id, spend, None)
            .flatMap { result =>
              val message =
                createMessage(
                  Buy,
                  info.id,
                  info.price,
                  result.totalPrice / result.amount,
                  result.amount,
                  info.score,
                  latestScore
                )
              notificator.sendTradeInfo(message)
            }
            .handleErrorWith(error =>
              logger.warn(error)(s"Could not buy ${info.id} for money=$spend, operation skipped")
            )

      }
    }

    def createOrders(holdings: List[Holding]): F[Int] =
      if (holdings.isEmpty) 0.pure[F]
      else
        for {
          currentOrders <- tradingClient.ordersList()
          holdingsToOrder = holdings.filter(holding =>
            !currentOrders.filter(_.side == Side.Buy).map(_.tokenId).contains(holding.asset)
          )
          _ <- holdingsToOrder.traverse_(holding => tradingClient.sellOrder(holding.asset, holding.size, 0.99))
        } yield (currentOrders ++ holdingsToOrder).length

    def currentHoldingsF(): F[List[Holding]] = {
      for {
        positions <- tradingClient.positions().map(NonEmptyList.fromList)
        infos <-
          positions match {
            case None      => Map.empty[String, TokenInfoShort].pure[F]
            case Some(lst) => tokensInfoRegistry.tokensInfoForTokens(lst.map(_.asset))
          }
      } yield positions.fold(List.empty[Position])(_.toList).map(position => toHolding(position, infos))
    }

    for {
      now             <- Clock[F].realTimeInstant
      topInfos        <- tokensInfoRegistry.topTokensInfo
      currentHoldings <- currentHoldingsF().map(_.sortBy(holding => -holding.efficiency))
      latestScores    <- tokensInfoRegistry.latestScores

      (ourPositive, ourNegative) =
        currentHoldings
          .partition(_.efficiency > 0)

      (ourCloseToResolve, ourNew) = ourPositive.partition(isCloseToResolve(now, _))
      (ourToSell, ourToStay) = ourNew.partition { holding =>
        val latestScore = latestScores.getOrElse(holding.asset, BigDecimal(0))
        latestScore < 0 && -latestScore / holding.score > 0.1
      }

      _ <- ourNegative.traverse_(holding =>
        trySell(holding, holding.size, latestScores.getOrElse(holding.asset, BigDecimal(0)))
      )
      _ <- ourToSell.traverse_(holding =>
        trySell(holding, holding.size, latestScores.getOrElse(holding.asset, BigDecimal(0)))
      )
      _             <- createOrders(ourCloseToResolve)
      balance       <- tradingClient.balance()
      portfolioValue = ourPositive.map(_.totalPrice).sum
      totalBalance   = portfolioValue + balance
      targetMarkets  = topInfos.filter(info => !ourPositive.exists(_.asset != info._1))
      _ <- targetMarkets.traverse {
        case (id, info) =>
          tryBuy(info, totalBalance / marketsAmount, latestScores.getOrElse(id, BigDecimal(0)))
      }
      _ <- targetMarkets.traverse_(info => tokensInfoRegistry.setBuyTimeBuyPrice(info._1, now, info._2.price))
    } yield ()
  }

  override def updateEvery(duration: FiniteDuration): F[Unit] =
    performOperations >> Clock[F].sleep(duration) >> updateEvery(duration)
}

object TraderLazy {
  def of[F[_]: Async](
    registry: TokensInfoRegistry[F],
    tradingClient: TradingClient[F],
    marketsAmount: Int,
    secondsBeforeResolve: Int,
    notificator: InsiderTelegramBot[F]
  ): F[TraderLazy[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TraderLazy[F](registry, logger, tradingClient, secondsBeforeResolve, marketsAmount, notificator)
      )

}
