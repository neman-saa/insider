package org.github.insider.realtime.traders.scorevector

import cats.Applicative
import cats.effect.{Clock, Sync}
import cats.effect.kernel.Async
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.realtime.traders.TraderConfig
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.github.insider.realtime.tokens.{EffectiveTokenInfo, TokenId, TokenInfo, TokensInfoRegistry}
import org.github.insider.realtime.traders.scorevector.OperationAuditLog.{BuyAuditLog, SellAuditLog}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.annotation.tailrec
import scala.util.Try

class ScoreVectorTrader[F[_]: Async](
  tradingClient: TradingClient[F],
  tokensInfoRegistry: TokensInfoRegistry[F],
  opAuditor: OperationsAuditor[F],
  traderConfig: TraderConfig,
)(logger: Logger[F]) {

  def trade: F[Unit] = {
    tokensInfoRegistry.recentScoreChangesLength.flatMap { recentScoreChangesLength =>
      if (recentScoreChangesLength < traderConfig.recentScoreChangesBlocksLength)
        Applicative[F].unit
      else
        for {
          activePositions <- tradingClient.positions()

          recentScoreChanges <- tokensInfoRegistry.recentScoreChanges

          sellCandidates     <- getSellCandidates(activePositions, recentScoreChanges)
          maybeSellAuditLogs <- sellCandidates.traverse(performSell)
          sellAuditLogs       = maybeSellAuditLogs.flatten
          _                  <- sellAuditLogs.traverse(opAuditor.audit)

          balance <- tradingClient.balance()

          buyCandidates     <- getBuyCandidates(balance, activePositions.size - sellAuditLogs.size, recentScoreChanges)
          maybeBuyAuditLogs <- buyCandidates.traverse(performBuy)
          buyAuditLogs       = maybeBuyAuditLogs.flatten
          _                 <- buyAuditLogs.traverse(opAuditor.audit)
        } yield ()
    }
  }

  private def getSellCandidates(
    activePositions: List[Position],
    recentScoreChanges: Map[TokenId, BigDecimal],
  ): F[List[SellCandidate]] = {
    activePositions
      .traverseFilter { position =>
        isSellCandidate(position, recentScoreChanges)
      }
      .map { positionsToSell =>
        positionsToSell.map(position => SellCandidate(position.asset, position.size))
      }
  }

  /*
   * Three sell triggers:
   * - market score decreased on longScoreDrawdownPercentThreshold percent from buy score within long period of time.
   *   It means market slowly loosing its potential
   * - market score decreased on shortScoreDrawdownPercentThreshold percent from buy score within short period of time.
   *   It means markets actively loosing its potential
   * - early exit for profitable token to free money
   *
   * Returns Some(position) if @param position is sell candidate, otherwise returns None
   * */
  private def isSellCandidate(
    position: Position,
    recentScoreChanges: Map[TokenId, BigDecimal],
  ): F[Option[Position]] = {
    opAuditor.lastAuditLogFor(position.asset).flatMap {
      case Some(buyLog: BuyAuditLog) =>
        tokensInfoRegistry.getTokenInfo(position.asset).flatMap {
          case Some(tokenInfo: TokenInfo) =>
            val shortScoreChangePercent: Option[BigDecimal] =
              recentScoreChanges.get(position.asset).flatMap { recentScoreChange =>
                Try(tokenInfo.score / (tokenInfo.score - recentScoreChange) - 1).toOption
              }
            val longScoreChangePercent: Option[BigDecimal] =
              Try(tokenInfo.score / tokenInfo.maxScore - 1).toOption

            val shortExceeded =
              shortScoreChangePercent.exists(_ <= -(traderConfig.shortScoreDrawdownPercentThreshold / 100))
            val longExceeded =
              longScoreChangePercent.exists(_ <= -(traderConfig.longScoreDrawdownPercentThreshold / 100))

            if (shortExceeded) {
              logger.info(s"Significant short score change for token ${position.asset}: $shortScoreChangePercent") >>
                position.some.pure[F]
            } else if (longExceeded) {
              logger.info(s"Significant long score change for token ${position.asset}: $longScoreChangePercent") >>
                position.some.pure[F]
            } else if (tokenInfo.price >= BigDecimal(0.99) && buyLog.singleTokenPrice < BigDecimal(0.99)) {
              logger.info(s"Early exit for profitable token ${position.asset}") >>
                position.some.pure[F]
            } else none[Position].pure[F]

          case None =>
            logger
              .info(s"Unexpected state: no token info found in registry for held position ${position.asset}")
              .map(_ => position.some)
        }

      case Some(sellLog: SellAuditLog) =>
        logger
          .info(s"Unexpected state: last audit log is sell for held position ${position.asset}")
          .map(_ => position.some)

      case None =>
        logger
          .info(s"Unexpected state: no audit logs for held position ${position.asset}")
          .map(_ => position.some)
    }
  }

  private def performSell(sellCandidate: SellCandidate): F[Option[SellAuditLog]] = {
    val sellAction = for {
      now             <- Clock[F].realTimeInstant
      tokenInfo       <- tokensInfoRegistry.getTokenInfo(sellCandidate.tokenId)
      sellOrderResult <- tradingClient.sell(sellCandidate.tokenId, sellCandidate.size, minPrice = None)
    } yield SellAuditLog(
      tokenId              = sellCandidate.tokenId,
      momentScore          = tokenInfo.map(_.score),
      totalPrice           = sellOrderResult.totalPrice,
      totalShares          = sellOrderResult.amount,
      prevSingleTokenPrice = tokenInfo.map(_.price),
      singleTokenPrice     = sellOrderResult.totalPrice / (sellOrderResult.amount / BigDecimal(1_000_000)),
      timestamp            = now,
    ).some

    sellAction.handleErrorWith { error =>
      logger.error(error)(s"Unsuccessful sell operation for $sellCandidate") as none[SellAuditLog]
    }
  }

  private def getBuyCandidates(
    balance: BigDecimal,
    activeMarketsCount: Int,
    recentScoreChanges: Map[TokenId, BigDecimal],
  ): F[List[BuyCandidate]] = {
    // leave 1 USD in balance to cover fees
    val tradingBalance = (balance - BigDecimal(1)).max(BigDecimal(0))

    val (marketsCountToBuy, quote) =
      getBuyMarketsCountWithQuotes(tradingBalance, traderConfig.maxActiveMarketsCount - activeMarketsCount)

    val quoteAmounts        = List.fill(n = marketsCountToBuy)(elem = quote)
    val clampedQuoteAmounts = quoteAmounts.filter(_ > traderConfig.minUsdForSingleMarket)

    for {
      topTokens          <- tokensInfoRegistry.topRecentTokensInfo
      buyCandidateTokens <- topTokens.traverseFilter(token => isBuyCandidate(token, recentScoreChanges))
    } yield clampedQuoteAmounts.zip(buyCandidateTokens).map {
      case (quote, buyCandidateToken) =>
        BuyCandidate(
          tokenId  = buyCandidateToken.id,
          money    = quote,
          maxPrice = (buyCandidateToken.price + traderConfig.maxBuyPriceAddCents).min(BigDecimal(0.99)).some,
        )
    }
  }

  @tailrec
  private def getBuyMarketsCountWithQuotes(balance: BigDecimal, maxMarketsCount: Int): (Int, BigDecimal) = {
    if (maxMarketsCount <= 0 || balance < traderConfig.minUsdForSingleMarket) (0, 0)
    else {
      val quote = balance / BigDecimal(maxMarketsCount)

      if (quote < traderConfig.minUsdForSingleMarket)
        getBuyMarketsCountWithQuotes(balance, maxMarketsCount / 2)
      else
        (maxMarketsCount, quote)
    }
  }

  private def isBuyCandidate(
    tokenInfo: EffectiveTokenInfo,
    recentScoreChanges: Map[TokenId, BigDecimal],
  ): F[Option[EffectiveTokenInfo]] =
    opAuditor.lastAuditLogFor(tokenInfo.id).flatMap {
      // consider to buy more if recent score shows positive vector
      case Some(buyLog: BuyAuditLog) =>
        none[EffectiveTokenInfo].pure[F]

      // buy first time or re-buy if recent score shows positive vector
      case _ =>
//        val shortScoreChangePercent: Option[BigDecimal] =
//          recentScoreChanges.get(tokenInfo.id).flatMap { recentScoreChange =>
//            Try(tokenInfo.score / (tokenInfo.score - recentScoreChange) - 1).toOption
//          }

//        if (shortScoreChangePercent.forall(_ > -(traderConfig.shortScoreDrawdownPercentThreshold / 100)))
        tokenInfo.some.pure[F]
//        else
//          none[EffectiveTokenInfo].pure[F]
    }

  private def performBuy(buyCandidate: BuyCandidate): F[Option[BuyAuditLog]] = {
    val buyAction = for {
      now            <- Clock[F].realTimeInstant
      tokenInfo      <- tokensInfoRegistry.getTokenInfo(buyCandidate.tokenId)
      buyOrderResult <- tradingClient.buy(buyCandidate.tokenId, buyCandidate.money, buyCandidate.maxPrice)
    } yield BuyAuditLog(
      tokenId              = buyCandidate.tokenId,
      momentScore          = tokenInfo.map(_.score),
      totalPrice           = buyOrderResult.totalPrice,
      totalShares          = buyOrderResult.amount,
      prevSingleTokenPrice = tokenInfo.map(_.price),
      singleTokenPrice     = buyOrderResult.totalPrice / (buyOrderResult.amount / BigDecimal(1_000_000)),
      timestamp            = now,
    ).some

    buyAction.handleErrorWith { error =>
      logger.error(error)(s"Unsuccessful buy operation for $buyCandidate") as none[BuyAuditLog]
    }
  }
}

object ScoreVectorTrader {
  def of[F[_]: Async](
    tradingClient: TradingClient[F],
    tokensInfoRegistry: TokensInfoRegistry[F],
    opAuditor: OperationsAuditor[F],
    traderConfig: TraderConfig,
  ): F[ScoreVectorTrader[F]] = {
    Slf4jLogger
      .create[F]
      .map(logger => new ScoreVectorTrader[F](tradingClient, tokensInfoRegistry, opAuditor, traderConfig)(logger))
  }
}
