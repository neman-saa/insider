package org.github.insider

import canoe.api.TelegramClient
import cats.effect.{IO, IOApp, Ref}
import cats.implicits.catsSyntaxTuple2Parallel
import fs2.concurrent.Topic
import org.github.insider.alchemy.TradesRealtimeFlow
import org.github.insider.alchemy.client.TransfersClientImpl
import org.github.insider.alchemy.domain.User
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.{AggregatedTradesRepositoryImpl, TradesRepositoryImpl}
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.leaderboard.{LeaderboardEntry, LeaderboardStrategy, Leaderboards, RoiLeaderboardStrategyCH, TradeNotification, WinRateLeaderboardStrategyCH}
import org.github.insider.notifications.services.TelegramNotificator
import org.github.insider.persistance.Database
import org.github.insider.polymarket.{EventsCached, EventsRealtimeFlow}
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.domain.Trade
import org.github.insider.polymarket.repository.{EventsImpl, MarketsImpl}
import org.github.insider.polymarket.workers.EventsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Instant, LocalDateTime}
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

      marketsImpl <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl  <- EventsImpl.of[IO](transactor).toResource

      transfersProcessor <- TransfersProcessorImpl.of[IO]().toResource

      tradeNotifications                    <- Topic[IO, TradeNotification].toResource
      implicit0(client: TelegramClient[IO]) <- TelegramClient[IO](config.telegram.token)
      _                                     <- TelegramNotificator.create[IO](tradeNotifications).start.toResource

      eventsWorker <- EventsExtractorWorkerGroup
        .of[IO](eventClient, marketsImpl, eventsImpl, limit = 100)(workersNumber = 5)
        .toResource
      tradesWorker <- TradeWorkerGroup
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          config.alchemy.ctfAddress,
          step = 300
        )(nWorkers = 5)
        .toResource
      eventsCached <- EventsCached.of[IO](eventClient)
      leaderboards <- Leaderboards.make[IO](
        strategies = List[LeaderboardStrategy[IO]](
          WinRateLeaderboardStrategyCH[IO](transactor),
        )
      )

      realtimeTrades <- TradesRealtimeFlow
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          config.alchemy,
          tradeNotifications,
          leaderboards,
          eventsCached
        )
        .toResource
      realtimeEvents <- EventsRealtimeFlow.of[IO](eventClient, eventsImpl, marketsImpl).toResource
    } yield (realtimeTrades, realtimeEvents, eventsWorker, tradesWorker)

    resource use {
      case (realtimeTrades, realtimeEvents, eventsWorker, tradesWorker) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          // _ <- eventsWorker.extractAllClosedEvents
          // _ <- tradesWorker.run(83_934_808, 84_295_753)

          _ <- (realtimeTrades.runForever, realtimeEvents.runForever).parTupled
        } yield ()
    }
  }
}
