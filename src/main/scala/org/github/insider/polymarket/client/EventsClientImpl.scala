package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import org.github.insider.polymarket.domain.{Event, Tag}
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

  override def getEventsByTag(tag: Tag, limit: Int, offset: Int): F[List[Event]] = {
    val uri: Uri =
      GammaApiHost
        .addSegment("events")
        .withQueryParam("tag_id", tag.id)
        .withQueryParam("limit", limit)
        .withQueryParam("offset", offset)
        .withQueryParam("order", "creationDate")

    client.get[List[Event]](uri) {
      case Status.Successful(response) =>
        response.attemptAs[List[Event]].value.flatMap {
          case Left(_) =>
            logger.error(s"Unable to parse getEventsByTag response. Request params: ${uri.params}").as(List.empty)
          case Right(events) =>
            events.pure[F]
        }
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
