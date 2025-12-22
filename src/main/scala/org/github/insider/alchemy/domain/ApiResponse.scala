package org.github.insider.alchemy.domain

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

final case class ApiResponse[A: Decoder](
  result: A,
)

object ApiResponse {
  implicit def decoder[A: Decoder]: Decoder[ApiResponse[A]] = deriveDecoder

  final case class GetAssetTransfersApiResponseBody(
    transfers: List[Transfer],
  )

  object GetAssetTransfersApiResponseBody {
    implicit val decoder: Decoder[GetAssetTransfersApiResponseBody] = deriveDecoder
  }
}
