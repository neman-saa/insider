package org.github.insider

import cats.effect.{IO, IOApp}
import org.github.insider.alchemy.client.TransfersClientImpl
import org.github.insider.alchemy.processors.TransfersProcessorImpl
import org.github.insider.alchemy.repository.TradesRepositoryImpl
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.configs.syntax.sourceOps
import org.github.insider.polymarket.persistance.{Database, DbMigrations}
import org.github.insider.polymarket.workers.TagsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config            <- ConfigSource.default.loadF[IO, MainConfig].toResource
      transactor        <- Database.postgresResource[IO](config.dbConfig)
      tradesRepository  <- TradesRepositoryImpl.of[IO](transactor).toResource
      client            <- EmberClientBuilder.default[IO].build
      eventClient       <- EventsClientImpl.of[IO](client).toResource
      tagsClient        <- TagsClientImpl.of[IO](client).toResource
      transfersClient   <- TransfersClientImpl.of[IO](client, config.alchemy.apiKey).toResource
      transfersProcessor = TransfersProcessorImpl()
      tradeWorkerGroup <- TradeWorkerGroup
        .of[IO](transfersClient, transfersProcessor, tradesRepository, config.alchemy.ctfAddress, 100)(25)
        .toResource
      _ <- DbMigrations.migrate[IO](config.dbConfig).toResource
    } yield (tagsClient, tradeWorkerGroup)

    resource use {
      case (tagsClient, tradeWorkerGroup) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

//          keywords = List("stock", "google", "apple", "revenue", "report")

//          tagsExtractor <- TagsExtractorWorkerGroup.of[IO](tagsClient)(workersNumber = 3)
//          relevantTags  <- tagsExtractor.getRelevantTags(keywords, limit = 100, maxDepth = 5000)

//          eventsPerTag <- relevantTags
//            .parTraverse { tag =>
//              eventClient.getEventsByTag(tag, 10, 0).map(events => tag -> events)
//            }
//            .map(_.toMap)

          _ <- tradeWorkerGroup.run(80801203, 80801603)
          _ <- logger.info("Shutting down application...")
        } yield ()
    }
  }
}
