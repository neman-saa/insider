package org.github.insider

import cats.effect.kernel.Resource
import cats.effect.{IO, IOApp, Ref}
import cats.syntax.all._
import com.comcast.ip4s.IpLiteralSyntax
import org.github.insider.alchemy.TradesRealtimeFlow
import org.github.insider.alchemy.client.{ApiKeysPool, TransfersClientImpl}
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.{AggregatedTradesRepositoryImpl, TradesRepositoryImpl}
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.strategy.RoiNoTradersStrategyCh
import org.github.insider.leaderboard.Leaderboards
import org.github.insider.notifications.services.InsiderTelegramBot
import org.github.insider.persistance.Database
import org.github.insider.polymarket.{EventsCached, EventsRealtimeFlow}
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl, TradingClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.repository.{EventsImpl, MarketsImpl}
import org.github.insider.polymarket.workers.EventsExtractorWorkerGroup
import org.github.insider.realtime.tokens.{TokensInfoRegistry, TokensInfoRepositoryImpl}
import org.github.insider.realtime.traders.scorevector.{OperationsAuditRepository, OperationsAuditor, ScoreVectorTrader}
import org.http4s.HttpRoutes
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.metrics.prometheus.PrometheusExportService
import org.http4s.server.{Router, Server}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import sttp.client4.httpclient.cats.HttpClientCatsBackend

import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config <- MainConfig.loadR[IO]

      transactor <- Database.makeTransactor[IO](config.dbConfig)

      _ <- httpServer

      pool                       <- ApiKeysPool.of[IO](config.alchemy.apiKeys).toResource
      tradesRepository           <- TradesRepositoryImpl.of[IO](transactor).toResource
      aggregatedTradesRepository <- AggregatedTradesRepositoryImpl.of[IO](transactor).toResource

      client          <- EmberClientBuilder.default[IO].withTimeout(5.minutes).withIdleConnectionTime(5.minutes).build
      eventClient     <- EventsClientImpl.of[IO](client).toResource
      transfersClient <- TransfersClientImpl.of[IO](client, pool).toResource
      tradingClient   <- TradingClientImpl.of[IO](client, config.polymarket).toResource

      marketsImpl <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl  <- EventsImpl.of[IO](transactor).toResource

      tokensInfoRepository <- TokensInfoRepositoryImpl.of[IO](transactor).toResource
      tokensInfoRegistry <- TokensInfoRegistry
        .withInit[IO](
          tokensInfoRepository,
          cleanUpPeriod = 5.minutes,
          config.wallets.secondsToSellBeforeResolve,
          config.trader.recentScoreChangesBlocksLength,
        )
        .toResource

      transfersProcessor = TransfersProcessorImpl(config.polygonContracts)

      followTokens <- Ref.empty[IO, Map[String, Set[String]]].toResource

      tgBackend <- HttpClientCatsBackend.resource[IO]()
      insiderBot = new InsiderTelegramBot[IO](config.telegram.botToken, config.telegram.chatId, tgBackend)(
        followTokens,
        tradingClient
      )
      _ <- runForeverTgPolling(insiderBot).start.toResource

      eventsCached <- EventsCached.of[IO](eventClient)
      leaderboards <- Leaderboards.make[IO, AdvancedLeaderboardEntry](
        strategy = RoiNoTradersStrategyCh[IO](transactor),
        tradesRepository
      )

      operationsAuditRepository <- OperationsAuditRepository.OperationsAuditRepositoryImpl.of[IO](transactor).toResource
      opAuditor                 <- OperationsAuditor.of[IO](operationsAuditRepository).toResource

      trader <- ScoreVectorTrader
        .of(
          tradingClient,
          tokensInfoRegistry,
          opAuditor,
          config.trader,
        )
        .toResource

      realtimeTrades <- TradesRealtimeFlow
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          tokensInfoRepository,
          insiderBot,
          leaderboards,
          eventsCached,
          tokensInfoRegistry,
          config.polygonContracts,
          trader,
          config.trader.enabled,
        )
        .toResource
      realtimeEvents <- EventsRealtimeFlow.of[IO](eventClient, eventsImpl, marketsImpl).toResource
    } yield (realtimeTrades, realtimeEvents)

    resource use {
      case (realtimeTrades, realtimeEvents) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          _ <- (
            realtimeTrades.runForever,
            realtimeEvents.runForever,
          ).parTupled
        } yield ()
    }
  }

  private def runForeverTgPolling(tgBot: InsiderTelegramBot[IO]): IO[Unit] =
    tgBot.startPolling().handleErrorWith(_ => IO.sleep(10.seconds)).foreverM

  private def httpServer: Resource[IO, Server] = {
    val meteredRouter: Resource[IO, HttpRoutes[IO]] =
      for {
        metricsService <- PrometheusExportService.build[IO]
        router = Router[IO](
          "/" -> metricsService.routes
        )
      } yield router

    meteredRouter.flatMap { router =>
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withPort(port"8080")
        .withHttpApp(router.orNotFound)
        .withShutdownTimeout(10.seconds)
        .withLogger(Slf4jLogger.getLogger[IO])
        .build
    }
  }
}
