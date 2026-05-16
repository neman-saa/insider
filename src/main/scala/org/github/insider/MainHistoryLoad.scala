package org.github.insider

import cats.effect.std.Env
import cats.effect.{IO, IOApp}
import org.github.insider.alchemy.client.{ApiKeysPool, TransfersClientImpl}
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.{AggregatedTradesRepositoryImpl, TradesRepositoryImpl}
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.persistance.Database
import org.github.insider.polymarket.client.EventsClientImpl
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.repository.{EventsImpl, MarketsImpl}
import org.github.insider.polymarket.workers.EventsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

object MainHistoryLoad extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config <- MainConfig.loadR[IO]

      maybeIsOldContracts <- Env[IO].get("IS_OLD_CONTRACTS").toResource
      isOldContracts <- IO
        .fromOption(maybeIsOldContracts.map(_.toBoolean))(
          new Throwable("No IS_OLD_CONTRACTS defined")
        )
        .toResource

      transactor <- Database.makeTransactor[IO](config.dbConfig)

      tradesRepository           <- TradesRepositoryImpl.of[IO](transactor).toResource
      aggregatedTradesRepository <- AggregatedTradesRepositoryImpl.of[IO](transactor).toResource

      client <- EmberClientBuilder.default[IO].withTimeout(5.minutes).withIdleConnectionTime(5.minutes).build
      alchemyApiKeysPool <- ApiKeysPool.of[IO](config.alchemy.apiKeys).toResource
      transfersClient    <- TransfersClientImpl.of[IO](client, alchemyApiKeysPool).toResource
      eventClient        <- EventsClientImpl.of[IO](client).toResource
      marketsImpl        <- MarketsImpl.of[IO](transactor).toResource
      eventsImpl         <- EventsImpl.of[IO](transactor).toResource

      eventsWorker <- EventsExtractorWorkerGroup
        .of[IO](eventClient, marketsImpl, eventsImpl, limit = 50)(workersNumber = 5)
        .toResource

      contracts          = if (isOldContracts) config.oldPolygonContracts else config.polygonContracts
      transfersProcessor = TransfersProcessorImpl(contracts)

      tradesWorker <- TradeWorkerGroup
        .of[IO](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedTradesRepository,
          contracts,
        )(step = 50, nWorkers = 5)
        .toResource
    } yield (tradesWorker, eventsWorker)

    resource use {
      case (tradesWorker, eventsWorker) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          maybeFromBlock <- Env[IO].get("TRADES_HISTORY_LOAD_START_BLOCK")
          fromBlock <- IO.fromOption(maybeFromBlock.map(_.toInt))(
            new Throwable("No TRADES_HISTORY_LOAD_START_BLOCK defined")
          )
          maybeToBlock <- Env[IO].get("TRADES_HISTORY_LOAD_END_BLOCK")
          toBlock <- IO.fromOption(maybeToBlock.map(_.toInt))(
            new Throwable("No TRADES_HISTORY_LOAD_END_BLOCK defined")
          )

          // _ <- eventsWorker.extractAllClosedEvents
          _ <- tradesWorker.run(fromBlock, toBlock)
        } yield ()
    }
  }
}
