package org.github.insider

import cats.effect.{IO, IOApp, Ref}
import org.github.insider.alchemy.TradesRealtimeFlow
import org.github.insider.alchemy.client.TransfersClientImpl
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
import org.github.insider.realtime.wallets.Wallet
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import sttp.client4.httpclient.cats.HttpClientCatsBackend

import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config <- MainConfig.loadR[IO]

      transactor <- Database.makeTransactor[IO](config.dbConfig)

      tradesRepository           <- TradesRepositoryImpl.of[IO](transactor).toResource
      aggregatedTradesRepository <- AggregatedTradesRepositoryImpl.of[IO](transactor).toResource

      client          <- EmberClientBuilder.default[IO].withTimeout(5.minutes).withIdleConnectionTime(5.minutes).build
      eventClient     <- EventsClientImpl.of[IO](client).toResource
      tagsClient      <- TagsClientImpl.of[IO](client).toResource
      transfersClient <- TransfersClientImpl.of[IO](client, config.alchemy.apiKey).toResource
      tradingClient   <- TradingClientImpl.of[IO](client, config.polymarket).toResource

      marketsImpl <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl  <- EventsImpl.of[IO](transactor).toResource

      tokensInfoRepository <- TokensInfoRepositoryImpl.of[IO](transactor).toResource
      tokensInfoRegistry <- TokensInfoRegistry
        .withInit[IO](
          tokensInfoRepository,
          cleanUpPeriod = 5.minutes,
          config.wallets.secondsToSellBeforeResolve,
          config.wallets.marketsAmount
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

      eventsWorker <- EventsExtractorWorkerGroup
        .of[IO](eventClient, marketsImpl, eventsImpl, limit = 100)(workersNumber = 5)
        .toResource
      tradesWorker <- TradeWorkerGroup
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          config.polygonContracts,
        )(step = 100, nWorkers = 5)
        .toResource
      eventsCached <- EventsCached.of[IO](eventClient)
      leaderboards <- Leaderboards.make[IO, AdvancedLeaderboardEntry](
        strategy = RoiNoTradersStrategyCh[IO](transactor),
        tradesRepository
      )

      wallet <- Wallet
        .of(
          tokensInfoRegistry,
          tradingClient,
          config.wallets.marketsAmount,
          config.wallets.sellThresholdPercent
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
        )
        .toResource
      realtimeEvents <- EventsRealtimeFlow.of[IO](eventClient, eventsImpl, marketsImpl).toResource
    } yield (realtimeTrades, realtimeEvents, eventsWorker, tradesWorker, wallet, config)

    resource use {
      case (realtimeTrades, realtimeEvents, eventsWorker, tradesWorker, wallet, config) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          // _ <- eventsWorker.extractAllClosedEvents
          // _ <- tradesWorker.run(86_200_414, 86_208_414)

          _ <- (
            realtimeTrades.runForever,
            realtimeEvents.runForever,
            wallet.updateEvery(config.wallets.updateEveryMinutes.minutes)
          ).parTupled
        } yield ()
    }
  }

  private def runForeverTgPolling(tgBot: InsiderTelegramBot[IO]): IO[Unit] =
    tgBot.startPolling().handleErrorWith(_ => IO.sleep(10.seconds)).foreverM
}
