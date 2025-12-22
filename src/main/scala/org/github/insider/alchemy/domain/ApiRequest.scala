package org.github.insider.alchemy.domain

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

final case class ApiRequest[A: Encoder](
  jsonrpc: String,
  method: String,
  id: String,
  params: List[A],
)

object ApiRequest {
  implicit def encoder[A: Encoder]: Encoder[ApiRequest[A]] = deriveEncoder

  final case class GetAssetTransfersPayload(
    fromBlock: Option[String],
    toBlock: Option[String],
    fromAddress: Option[String],
    toAddress: Option[String],
    category: Set[TokenCategory],
    withMetadata: Option[Boolean],
  )

  object GetAssetTransfersPayload {
    implicit val encoder: Encoder[GetAssetTransfersPayload] =
      deriveEncoder[GetAssetTransfersPayload].mapJson(_.dropNullValues)
  }

  def getAssetTransfersRequest(
    fromBlock: Option[String],
    toBlock: Option[String],
    fromAddress: Option[String],
    toAddress: Option[String],
    category: Set[TokenCategory],
    withMetadata: Option[Boolean],
  ): ApiRequest[GetAssetTransfersPayload] = {
    val payload = GetAssetTransfersPayload(
      fromBlock    = fromBlock,
      toBlock      = toBlock,
      fromAddress  = fromAddress,
      toAddress    = toAddress,
      category     = category,
      withMetadata = withMetadata,
    )

    getAssetTransfersRequest(payload)
  }

  def getAssetTransfersRequest(payload: GetAssetTransfersPayload): ApiRequest[GetAssetTransfersPayload] =
    ApiRequest[GetAssetTransfersPayload](
      jsonrpc = "2.0",
      method  = "alchemy_getAssetTransfers",
      id      = "1",
      params  = List(payload),
    )
}
