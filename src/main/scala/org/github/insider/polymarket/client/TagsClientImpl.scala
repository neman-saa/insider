package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import org.github.insider.polymarket.domain.Tag
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.{Status, Uri}
import org.http4s.client._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

private class TagsClientImpl[F[_]: Async](
  client: Client[F],
  logger: Logger[F],
) extends TagsClient[F] {

  override def getTags(limit: Int, offset: Int): F[List[Tag]] = {
    val uri: Uri =
      GammaApiHost
        .addSegment("tags")
        .withQueryParam("limit", limit)
        .withQueryParam("offset", offset)
        .withQueryParam("order", "createdAt")

    client.get[List[Tag]](uri) {
      case Status.Successful(response) =>
        response.as[List[Tag]]
      case other =>
        logger.error(
          s"Unsuccessful response received while fetching tags [limit - $limit, offset - $offset]: $other"
        ) >> Async[F].raiseError(new Throwable("todo"))
    }
  }
}

object TagsClientImpl {
  def of[F[_]: Async](client: Client[F]): F[TagsClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = false, logHeaders = false)(client)

    Slf4jLogger.create[F].map(logger => new TagsClientImpl[F](clientWithLogging, logger))
  }
}
