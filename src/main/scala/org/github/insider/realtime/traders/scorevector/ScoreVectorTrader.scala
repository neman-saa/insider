package org.github.insider.realtime.traders.scorevector

import cats.effect.Clock
import cats.effect.kernel.Async
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.realtime.traders.TraderConfig
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Position
import org.github.insider.realtime.tokens.{EffectiveTokenInfo, TokenId, TokenInfo, TokensInfoRegistry}
import org.github.insider.realtime.traders.scorevector.OperationAuditLog.{BuyAuditLog, SellAuditLog}
import org.typelevel.log4cats.slf4j.Slf4jLogger

class ScoreVectorTrader[F[_]: Async](
  tradingClient: TradingClient[F],
  tokensInfoRegistry: TokensInfoRegistry[F],
  opAuditor: OperationsAuditor[F],
  traderConfig: TraderConfig,
)(logger: Logger[F]) {

  def trade: F[Unit] = {
    for {
      activePositions <- tradingClient.positions()

      recentScoreChanges <- tokensInfoRegistry.recentScoreChanges

      sellCandidates <- getSellCandidates(activePositions, recentScoreChanges)
      sellAuditLogs  <- sellCandidates.traverse(performSell)
      _              <- sellAuditLogs.flatten.traverse(opAuditor.audit)

      balance        <- tradingClient.balance()
      portfolioValue <- tradingClient.portfolioValue()

      buyCandidates <- getBuyCandidates(balance, portfolioValue)
      buyAuditLogs  <- buyCandidates.traverse(performBuy)
      _             <- buyAuditLogs.flatten.traverse(opAuditor.audit)
    } yield ()
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
   * Two sell triggers:
   * - market score decreased on longScoreDrawdownPercentThreshold percent from buy score within long period of time.
   *   It means market slowly loosing its potential
   * - market score decreased on shortScoreDrawdownPercentThreshold percent from buy score within short period of time.
   *   It means markets actively loosing its potential
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
              for {
                recentScoreChange <- recentScoreChanges.get(position.asset)
                momentScore       <- buyLog.momentScore
              } yield recentScoreChange / momentScore - 1

            val longScoreChangePercent: Option[BigDecimal] =
              buyLog.momentScore.map { momentScore =>
                tokenInfo.score / momentScore - 1
              }

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
      auditLog         = SellAuditLog(sellCandidate.tokenId, tokenInfo.map(_.score), sellOrderResult.totalPrice, now)
    } yield auditLog.some

    sellAction.handleErrorWith { error =>
      logger.error(error)(s"Unsuccessful sell operation for $sellCandidate") as none[SellAuditLog]
    }
  }

  // max score
  private def getBuyCandidates(
    balance: BigDecimal,
    portfolioValue: BigDecimal,
  ): F[List[BuyCandidate]] = {
    val maxUsdForSingleMarket = (balance + portfolioValue) * (traderConfig.maxTotalBalancePercentForSingleMarket / 100)

    val maxUsdMarketsCount = (balance quot maxUsdForSingleMarket).toInt
    val reminder           = balance % maxUsdForSingleMarket

    // distribution of available balance to new markets
    val quoteAmounts        = List.fill(n = maxUsdMarketsCount)(elem = maxUsdForSingleMarket) :+ reminder
    val clampedQuoteAmounts = quoteAmounts.filter(price => price > traderConfig.minUsdForSingleMarket)

    for {
      topTokens          <- tokensInfoRegistry.topTokensInfo
      buyCandidateTokens <- topTokens.traverseFilter(isBuyCandidate)
      buyCandidates = clampedQuoteAmounts.zip(buyCandidateTokens).map {
        case (quote, buyCandidateToken) => BuyCandidate(buyCandidateToken.id, quote)
      }
      _ <- logger.info(s"Buy candidates: $buyCandidates")
    } yield buyCandidates
  }

  private def isBuyCandidate(
    tokenInfo: EffectiveTokenInfo,
  ): F[Option[EffectiveTokenInfo]] =
    opAuditor.lastAuditLogFor(tokenInfo.id).flatMap {
      // consider to buy more if recent score shows positive vector
      case Some(buyLog: BuyAuditLog) => none[EffectiveTokenInfo].pure[F]
      // consider to re-buy if recent score shows positive vector
      case Some(sellLog: SellAuditLog) => none[EffectiveTokenInfo].pure[F]
      case None                        => tokenInfo.some.pure[F]
    }

  private def performBuy(buyCandidate: BuyCandidate): F[Option[BuyAuditLog]] = {
    val buyAction = for {
      now            <- Clock[F].realTimeInstant
      tokenInfo      <- tokensInfoRegistry.getTokenInfo(buyCandidate.tokenId)
      buyOrderResult <- tradingClient.buy(buyCandidate.tokenId, buyCandidate.money, maxPrice = None)
      auditLog        = BuyAuditLog(buyCandidate.tokenId, tokenInfo.map(_.score), buyOrderResult.totalPrice, now)
    } yield auditLog.some

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
