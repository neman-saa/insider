package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import org.github.insider.polymarket.domain.{Event, Market, Tag}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.{Status, Uri}
import org.http4s.client._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.github.insider.polymarket.codecs.CustomCodecs._

import java.time.Instant

private class EventsClientImpl[F[_]: Async](
  client: Client[F],
  logger: Logger[F],
) extends EventsClient[F] {

  override def getEventByToken(token: String): F[Option[Event]] =
    getMarkets(baseUriMarket(1, 0).withQueryParam("clob_token_ids", List(token))).map {
      case List(market) => market.events.flatMap(_.headOption.map(_.copy(markets = Some(List(market)))))
    }

  override def getEventsByMaxEndDate(maxEndDate: Instant, limit: Int, offset: Int): F[List[Event]] = getEvents(
    baseUri(limit, offset).withQueryParam("end_date_max", maxEndDate.toString)
  )

  override def getEventsByTag(tag: Tag, limit: Int, offset: Int): F[List[Event]] =
    getEvents(baseUri(limit, offset).withQueryParam("tag_id", tag.id))

  override def getLastClosedEvents(limit: Int, offset: Int = 0): F[List[Event]] =
    getEvents(
      baseUri(limit, offset)
        .withQueryParam("ascending", "false")
        .withQueryParam("order", "closedTime")
        .withQueryParam("closed", "true")
    )

  private def baseUri(limit: Int, offset: Int): Uri =
    GammaApiHost
      .addSegment("events")
      .withQueryParam("limit", limit)
      .withQueryParam("offset", offset)
      .withQueryParam("order", "createdAt")

  private def baseUriMarket(limit: Int, offset: Int): Uri =
    GammaApiHost
      .addSegment("markets")
      .withQueryParam("limit", limit)
      .withQueryParam("offset", offset)
      .withQueryParam("order", "createdAt")

  private def getEvents(uri: Uri): F[List[Event]] = {
    client.get[List[Event]](uri) {
      case Status.Successful(response) =>
        response.attemptAs[List[Event]].value.flatMap {
          case Left(e) =>
            logger
              .error(s"Unable to parse getEvents response. Request params: ${uri.params}. Error: ${e.message}")
              .as(List.empty)
          case Right(events) =>
            events.pure[F]
        }
      case other =>
        logger.error(s"Unsuccessful response received while fetching events: $other") >>
          Async[F].raiseError(new Throwable("todo"))
    }
  }

  private def getMarkets(uri: Uri): F[List[Market]] = {
    client.get[List[Market]](uri) {
      case Status.Successful(response) =>
        response.attemptAs[List[Market]].value.flatMap {
          case Left(e) =>
            logger
              .error(s"Unable to parse getMarkets response. Request params: ${uri.params}. Error: ${e.message}")
              .as(List.empty)
          case Right(markets) =>
            markets.pure[F]
        }
      case other =>
        logger.error(s"Unsuccessful response received while fetching markets: $other") >>
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
