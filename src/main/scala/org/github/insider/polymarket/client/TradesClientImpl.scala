package org.github.insider.polymarket.client

import cats.syntax.all._
import cats.effect.Async
import org.github.insider.polymarket.domain.Trade
import org.http4s.Status
import org.http4s.client.{Client, middleware}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

private class TradesClientImpl[F[_]: Async](
  client: Client[F],
  logger: Logger[F],
) extends TradesClient[F] {

  override def getTradesHistoryByMarket(conditionId: String, offset: Int, limit: Int): F[List[Trade]] = {
    val uri = DataApiHost
      .addSegment("trades")
      .withQueryParam("market", conditionId)
      .withQueryParam("limit", limit)
      .withQueryParam("offset", offset)
      .withQueryParam("order", "createdAt")

    client.get[List[Trade]](uri) {
      case Status.Successful(response) =>
        response.as[List[Trade]]
      case other =>
        logger.error(
          s"Unsuccessful response received while fetching trades history for market $conditionId: $other"
        ) >> Async[F].raiseError(new RuntimeException("Failed to fetch trades history"))
    }
  }
}

object TradesClientImpl {
  def of[F[_]: Async](client: Client[F]): F[TradesClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = false, logHeaders = true)(client)

    Slf4jLogger.create[F].map(logger => new TradesClientImpl[F](clientWithLogging, logger))
  }
}
