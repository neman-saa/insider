package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import org.github.insider.polymarket.domain.Event
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.{Status, Uri}
import org.http4s.client._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.LocalDateTime

private class EventsClientImpl[F[_]: Async](
  client: Client[F],
  logger: Logger[F],
) extends EventsClient[F] {

  override def getEvents(startDateMax: LocalDateTime, endDateMax: LocalDateTime): F[List[Event]] = {
    val uri: Uri =
      GammaApiHost
        .addSegment("events")
        .withQueryParam("start_date_max", s"${startDateMax}Z")
        .withQueryParam("end_date_max", s"${startDateMax}Z")
        .withQueryParam("limit", 1)

    client.get[List[Event]](uri) {
      case Status.Successful(response) =>
        response.as[List[Event]]
      case other =>
        logger.error(s"Unsuccessful response received while fetching events: $other") >>
          Async[F].raiseError(new Throwable("todo"))
    }
  }
}

object EventsClientImpl {
  def of[F[_]: Async](client: Client[F]): F[EventsClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = false, logHeaders = true)(client)

    Slf4jLogger.create[F].map(logger => new EventsClientImpl[F](clientWithLogging, logger))
  }
}
