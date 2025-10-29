package org.github.insider

import cats.effect.{IO, IOApp}
import org.github.insider.polymarket.client.EventsClientImpl
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.LocalDateTime

object Main extends IOApp.Simple {
  override def run: IO[Unit] = {
    val resource = for {
      client      <- EmberClientBuilder.default[IO].build
      eventClient <- EventsClientImpl.of[IO](client).toResource
    } yield eventClient

    resource use { eventClient =>
      for {
        logger <- Slf4jLogger.create[IO]
        _      <- logger.info("Application startup initiated after successful resource acquisition...")

        testStartDateMax <- IO.delay(LocalDateTime.now().minusDays(7))
        testStartEndMax  <- IO.delay(LocalDateTime.now().minusDays(1))

        events <- eventClient.getEvents(testStartDateMax, testStartEndMax)
        _      <- logger.debug(events.toString)

        _ <- logger.info("Shutting down application...")
      } yield ()
    }
  }
}
