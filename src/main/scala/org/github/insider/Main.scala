package org.github.insider

import canoe.api.TelegramClient
import cats.effect.{IO, IOApp, Ref}
import fs2.concurrent.Topic
import org.github.insider.alchemy.TradesRealtimeFlow
import org.github.insider.alchemy.client.TransfersClientImpl
import org.github.insider.alchemy.domain.User
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.{AggregatedTradesRepositoryImpl, TradesRepositoryImpl}
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.notifications.services.TelegramNotificator
import org.github.insider.persistance.Database
import org.github.insider.polymarket.EventsRealtimeFlow
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.domain.Trade
import org.github.insider.polymarket.repository.{EventsImpl, MarketsImpl}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config                     <- MainConfig.loadR[IO]
      transactor                 <- Database.makeTransactor[IO](config.dbConfig)
      tradesRepository           <- TradesRepositoryImpl.of[IO](transactor).toResource
      aggregatedTradesRepository <- AggregatedTradesRepositoryImpl.of[IO](transactor).toResource
      client                     <- EmberClientBuilder.default[IO].build
      eventClient                <- EventsClientImpl.of[IO](client).toResource
      tagsClient                 <- TagsClientImpl.of[IO](client).toResource
      transfersClient            <- TransfersClientImpl.of[IO](client, config.alchemy.apiKey).toResource
      marketsImpl                <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl                 <- EventsImpl.of[IO](transactor).toResource
      transfersProcessor         <- TransfersProcessorImpl.of[IO]().toResource
      leaderboard                <- Ref.empty[IO, List[User]].toResource
      _ <-
        fs2
          .Stream
          .awakeEvery[IO](6.hours)
          .evalMap(_ =>
            for {
              newLeaderboard <- tradesRepository.leaderboard
              _              <- leaderboard.set(newLeaderboard)
            } yield ()
          )
          .compile
          .drain
          .start
          .toResource
      importantTrades <- Topic[IO, Trade].toResource
      client          <- EmberClientBuilder.default[IO].withTimeout(5.minutes).withIdleConnectionTime(5.minutes).build
      eventClient     <- EventsClientImpl.of[IO](client).toResource
      tagsClient      <- TagsClientImpl.of[IO](client).toResource
      transfersClient <- TransfersClientImpl.of[IO](client, config.alchemy.apiKey).toResource
      marketsImpl     <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl      <- EventsImpl.of[IO](transactor).toResource
      transfersProcessor <- TransfersProcessorImpl.of[IO]().toResource
      tradesRealtimeFlow <- TradesRealtimeFlow
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          config.alchemy,
          importantTrades,
          leaderboard
        )
        .toResource
      implicit0(client: TelegramClient[IO]) <- TelegramClient[IO](config.telegram.token)
      _                                     <- TelegramNotificator.create[IO](importantTrades).start.toResource
      tradesWorkerGroup <- TradeWorkerGroup
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          config.alchemy.ctfAddress,
          100
        )(5)
        .toResource
    } yield ()

    resource use { _ =>
      for {
        logger <- Slf4jLogger.create[IO]
        _      <- logger.info("Application started after successful resource acquisition...")
        // _ <- eventWorkerGroup.getCollectedEvents(Instant.parse("2026-01-01T00:00:00Z"), limit = 100, maxDepth = 150000)
        /**
          * Start with the following range: 66157355 - 81051370 (the whole 2025 year) Block numbers were extracted using
          * https://docs.etherscan.io/api-reference/endpoint/getblocknobytime
          */
        // _ <- tradesFlow.run(77_481_300, 81_500_000, 1000) // result in 78_763_244 trades
      // _ <- DbMigrations.migrate[IO](config.dbConfig).toResource // Neither flyway nor Liquibase support CH migrations, write custom tool
//      eventWorkerGroup <- EventsExtractorWorkerGroup
//        .of[IO](eventClient, marketsImpl, eventsImpl)(workersNumber = 3)
//        .toResource
    } yield (eventsRealtimeFlow)

    resource use {
      case (eventsRealtimeFlow) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          // _ <- eventWorkerGroup.getCollectedEvents(Instant.parse("2026-03-10T00:00:00Z"), limit = 1000, maxDepth = 500_000)

          // _ <- tradesWorkerGroup.run(83_300_500, 83_724_000)

          // _ <- tradesRealtimeFlow.runForever

        // _ <- tradesWorkerGroup.run(51_500_000, 83_599_513)
        // _ <- tradesRealtimeFlow.runForever
        _ <- logger.info("Shutting down application...")
      } yield ()
    }
  }
}
