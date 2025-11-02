package org.github.insider

import cats.effect.{IO, IOApp}
import cats.syntax.all._
import org.github.insider.polymarket.client.{EventsClientImpl, TagsClientImpl}
import org.github.insider.polymarket.workers.TagsExtractorWorkerGroup
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      client      <- EmberClientBuilder.default[IO].build
      eventClient <- EventsClientImpl.of[IO](client).toResource
      tagsClient  <- TagsClientImpl.of[IO](client).toResource
    } yield (eventClient, tagsClient)

    resource use {
      case (eventClient, tagsClient) =>
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

          _ <- logger.info("Shutting down application...")
        } yield ()
    }
  }
}
