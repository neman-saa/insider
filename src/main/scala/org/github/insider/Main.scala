package org.github.insider

import cats.effect.{IO, IOApp}
import org.github.insider.alchemy.TradesRealtimeFlow
import org.github.insider.alchemy.client.TransfersClientImpl
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.{AggregatedTradesRepositoryImpl, TradesRepositoryImpl}
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.persistance.Database
import org.github.insider.polymarket.EventsRealtimeFlow
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.repository.{EventsImpl, MarketsImpl}
import org.github.insider.polymarket.workers.EventsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
//      tradesRealtimeFlow <- TradesRealtimeFlow
//        .of[IO](transfersClient, transfersProcessor, tradesRepository, aggregatedTradesRepository, config.alchemy)
//        .toResource
//      tradesWorkerGroup <- TradeWorkerGroup
//        .of[IO](
//          transfersClient,
//          transfersProcessor,
//          tradesRepository,
//          aggregatedTradesRepository,
//          config.alchemy.ctfAddress,
//          1000
//        )(2)
//        .toResource
      eventsRealtimeFlow <- EventsRealtimeFlow.of[IO](eventClient, eventsImpl, marketsImpl).toResource
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
          // _ <- eventWorkerGroup.getCollectedEvents(Instant.parse("2026-01-01T00:00:00Z"), limit = 100, maxDepth = 150000)
          /**
            * Start with the following range: 66157355 - 81051370 (the whole 2025 year) Block numbers were extracted
            * using https://docs.etherscan.io/api-reference/endpoint/getblocknobytime
            */
          // _ <- tradesFlow.run(77_481_300, 81_500_000, 1000) // result in 78_763_244 trades

          _ <- eventsRealtimeFlow.runForever

          _ <- logger.info("Shutting down application...")
        } yield ()
    }
  }
}
