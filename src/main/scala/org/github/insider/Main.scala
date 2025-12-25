package org.github.insider

import cats.effect.{IO, IOApp}
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClientImpl
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.services.TradesImpl
import org.github.insider.alchemy.workers.TradeWorkerGroup
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.configs.MainConfig
import org.github.insider.polymarket.configs.syntax.sourceOps
import org.github.insider.polymarket.workers.TagsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      config           <- ConfigSource.default.loadF[IO, MainConfig].toResource
      client           <- EmberClientBuilder.default[IO].build
      eventClient      <- EventsClientImpl.of[IO](client).toResource
      tagsClient       <- TagsClientImpl.of[IO](client).toResource
      transfersClient  <- TransfersClientImpl.of[IO](client, config.alchemy.apiKey).toResource
      trades           <- TradesImpl.of[IO](config.alchemy.ctfAddress, config.alchemy.burnMintAddress).toResource
      tradeWorkerGroup <- TradeWorkerGroup.of[IO](transfersClient, trades, config.alchemy.ctfAddress, 25)(1).toResource
    } yield (eventClient, tagsClient, transfersClient, tradeWorkerGroup)

    resource use {
      case (eventClient, tagsClient, transfersClient, tradeWorkerGroup) =>
        for {
          logger <- Slf4jLogger.create[IO]
          _      <- logger.info("Application started after successful resource acquisition...")

          keywords = List("stock", "google", "apple", "revenue", "report")

          tagsExtractor <- TagsExtractorWorkerGroup.of[IO](tagsClient)(workersNumber = 3)
          /*relevantTags  <- tagsExtractor.getRelevantTags(keywords, limit = 100, maxDepth = 5000)*/

          // For testing purposes, 10 events are fetched for each tag.
          /*eventsPerTag <- relevantTags
            .parTraverse { tag =>
              eventClient.getEventsByTag(tag, 10, 0).map(events => tag -> events)
            }
            .map(_.toMap)*/

          // For testing purposes, all transfers from 0x40B1581 block are fetched with particular params
          /*transfers <- transfersClient.getAssetTransfers(
            fromBlock    = Some("0x40B1581"),
            toBlock      = Some("0x40B1581"),
            fromAddress  = Some("0xc5d563a36ae78145c45a50134d48a1215220f80a"),
            toAddress    = None,
            category     = Set(ERC20, ERC1155),
            withMetadata = None,
            page         = None
          )*/

          _ <- tradeWorkerGroup.run(80701191, 80701192)
          _ <- logger.info("Shutting down application...")
        } yield ()
    }
  }
}
