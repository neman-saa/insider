package org.github.insider

import cats.effect.{IO, IOApp}
import cats.syntax.all._
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl, TradesClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.configs.syntax.sourceOps
import org.github.insider.polymarket.persistance.{Database, DbMigrations, MarketsImpl}
import org.github.insider.polymarket.workers.{TagsExtractorWorkerGroup, TradeExtractorWorkerGroup}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config      <- ConfigSource.default.loadF[IO, MainConfig].toResource
      transactor  <- Database.postgresResource[IO](config.dbConfig)
      marketsDb     <- MarketsImpl.of[IO](transactor).toResource
      client      <- EmberClientBuilder.default[IO].build
      eventClient <- EventsClientImpl.of[IO](client).toResource
      tagsClient  <- TagsClientImpl.of[IO](client).toResource
      tradeClient <- TradesClientImpl.of[IO](client).toResource
      _ <- DbMigrations.migrate[IO](config.dbConfig).toResource
    } yield (eventClient, tagsClient, tradeClient, marketsDb)

    resource use {
      case (eventClient, tagsClient, tradeClient, marketsDb) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          keywords = List("stock", "google", "apple", "revenue", "report")

          tagsExtractor <- TagsExtractorWorkerGroup.of[IO](tagsClient)(workersNumber = 3)
          relevantTags  <- tagsExtractor.getRelevantTags(keywords, limit = 100, maxDepth = 5000)

          eventsPerTag <- relevantTags
            .parTraverse { tag =>
              eventClient.getEventsByTag(tag, 10, 0).map(events => tag -> events)
            }
            .map(_.toMap)
          _ <- logger.info(eventsPerTag.toString)

          tradesExtractor <- TradeExtractorWorkerGroup.of[IO](100, tradeClient)

          allMarkets = eventsPerTag.values.flatten.flatMap(_.markets).toList
          properMarkets <- tradesExtractor.tradesByAllMarkets(allMarkets, maxDepth = None, limit = 1000)

          _ <- logger.info(s"Found ${properMarkets.length} markets: ${properMarkets.map(_._1)}")
          _ <- logger.info("Started persist markets")

          _ <- properMarkets.traverse(market => marketsDb.addMarket(market._1, market._2))

          _ <- logger.info("Finished persisting markets")
          _ <- logger.info("Shutting down application...")
        } yield ()
    }
  }
}
