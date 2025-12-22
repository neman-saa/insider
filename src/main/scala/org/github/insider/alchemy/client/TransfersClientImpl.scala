package org.github.insider.alchemy.client

import cats.effect.Async
import cats.syntax.all._
import org.github.insider.alchemy.domain.ApiResponse.GetAssetTransfersApiResponseBody
import org.github.insider.alchemy.domain.{ApiRequest, ApiResponse, TokenCategory, Transfer}
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.client._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

private class TransfersClientImpl[F[_]: Async](
  client: Client[F],
  logger: Logger[F],
  apiKey: String,
) extends TransfersClient[F] {

  override def getAssetTransfers(
    fromBlock: Option[String],
    toBlock: Option[String],
    fromAddress: Option[String],
    toAddress: Option[String],
    category: Set[TokenCategory],
    withMetadata: Option[Boolean],
  ): F[List[Transfer]] = {
    val uri: Uri =
      PolygonMainnetHost
        .addSegment("v2")
        .addSegment(apiKey)

    val requestBody = ApiRequest.getAssetTransfersRequest(
      fromBlock    = fromBlock,
      toBlock      = toBlock,
      fromAddress  = fromAddress,
      toAddress    = toAddress,
      category     = category,
      withMetadata = withMetadata,
    )

    val request: Request[F] = Request[F](
      method = Method.POST,
      uri    = uri,
    ).withEntity(requestBody)

    client.run(request).use {
      case Status.Successful(response) =>
        response.attemptAs[ApiResponse[GetAssetTransfersApiResponseBody]].value.flatMap {
          case Left(_) =>
            logger.error(s"Unable to parse transfers response. Request params: ${uri.params}").as(List.empty)
          case Right(transfersApiResponse) =>
            transfersApiResponse.result.transfers.pure[F]
        }
      case other =>
        logger.error(s"Unsuccessful response received while fetching transfers: $other") >>
          Async[F].raiseError(new Throwable("todo"))
    }
  }
}

object TransfersClientImpl {
  def of[F[_]: Async](client: Client[F], apiKey: String): F[TransfersClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = true, logHeaders = true)(client)

    Slf4jLogger.create[F].map(logger => new TransfersClientImpl[F](clientWithLogging, logger, apiKey))
  }
}
