package org.github.insider.realtime.wallets

import cats.effect.{Async, Clock}
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.polymarket.domain.{BuyOrderResult, Position, SellOrderResult, Side}
import org.github.insider.realtime.tokens.{TokenInfoShort, TokensInfoRegistry}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.notifications.services.InsiderTelegramBot
import org.github.insider.polymarket.domain
import org.github.insider.polymarket.domain.Side._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

class TraderLazy[F[_]: Async] private (
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
    )
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

    def toHolding(position: Position, tokenInfos: List[TokenInfoShort]): Holding = {
      val info = tokenInfos
        .find(_.id == position.asset).getOrElse(TokenInfoShort(position.asset, -1, None, 0, Instant.now, 0))
      Holding(position.asset, position.size, info.price, info.efficiency, info.buyTime, info.resolveDate, info.score)
    }
    def trySell(holding: Holding, shares: BigDecimal, latestScore: BigDecimal): F[Option[SellOrderResult]] =
      if (shares <= 0) none[domain.SellOrderResult].pure[F]
      else
        tradingClient
          .sell(holding.asset, shares, None)
          .flatTap { result =>
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
          .map(Option.apply)
          .handleErrorWith(error =>
            logger.warn(error)(s"Could not sell ${holding.asset} shares=$shares, operation skipped") >> None.pure[F]
          )

    def sellAll(holdings: List[Holding], latestScores: Map[String, BigDecimal]): F[BigDecimal] =
      holdings
        .traverse(holding => trySell(holding, holding.size, latestScores.getOrElse(holding.asset, BigDecimal(0))))
        .map(_.flatten.map(_.totalPrice).sum)

    def tryBuy(info: TokenInfoShort, money: BigDecimal, latestScore: BigDecimal): F[Option[BuyOrderResult]] = {
      tradingClient.balance().flatMap { currentBalance =>
        val spend = money min currentBalance
        if (spend < 1) none[domain.BuyOrderResult].pure[F]
        else
          tradingClient
            .buy(info.id, spend, None)
            .flatTap { result =>
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
            .map(Option.apply)
            .handleErrorWith(error =>
              logger.warn(error)(s"Could not buy ${info.id} for money=$spend, operation skipped")
                >> none[domain.BuyOrderResult].pure[F]
            )

      }
    }

    def buyAll(infos: List[TokenInfoShort], scores: Map[String, BigDecimal], balance: BigDecimal): F[BigDecimal] =
      infos
        .traverse { info =>
          tryBuy(info, balance, scores.getOrElse(info.id, BigDecimal(0)))
        }
        .map(_.flatten.map(_.totalPrice).sum)

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
        positions <- tradingClient.positions()
        infos <- tokensInfoRegistry.tokensInfoForTokens(positions.map(_.asset))

      } yield positions.map(toHolding(_, infos))
    }

    def buySellOrderTokens(
      now: Instant,
      topInfos: List[TokenInfoShort],
      holdings: List[Holding],
      latestScores: Map[String, BigDecimal]
    ): (List[TokenInfoShort], List[Holding], List[Holding]) = {

      val (ourPositive, ourNegative) =
        holdings
          .partition(_.efficiency > 0)
      val (ourCloseToResolve, ourNew) = ourPositive.partition(isCloseToResolve(now, _))
      val (ourToSell, _) = ourNew.partition { holding =>
        val latestScore = latestScores.getOrElse(holding.asset, BigDecimal(0))
        latestScore < 0 && -latestScore / holding.score > 0.1
      }
      val targetMarkets = topInfos.filter(info => !ourPositive.exists(_.asset != info.id))

      (targetMarkets, ourToSell ++ ourNegative, ourCloseToResolve)
    }


    for {
      now             <- Clock[F].realTimeInstant
      topInfos        <- tokensInfoRegistry.topTokensInfo
      currentHoldings <- currentHoldingsF().map(_.sortBy(holding => -holding.efficiency))
      latestScores    <- tokensInfoRegistry.latestScores

      (toBuy, toSell, toOrder) = buySellOrderTokens(now, topInfos, currentHoldings, latestScores)

      _              <- createOrders(toOrder)
      _              <- sellAll(toSell, latestScores)

      portfolioValue <- tradingClient.portfolioValue()
      balance        <- tradingClient.balance()
      totalBalance    = balance + portfolioValue

      _ <- buyAll(toBuy, latestScores, totalBalance / marketsAmount)
      _ <- toBuy.traverse_(info => tokensInfoRegistry.setBuyTimeBuyPrice(info.id, now, info.price))
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
