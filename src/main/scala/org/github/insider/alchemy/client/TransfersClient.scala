package org.github.insider.alchemy.client

import org.github.insider.alchemy.domain.{TokenCategory, Transfer}

trait TransfersClient[F[_]] {
  def getAssetTransfers(
    fromBlock: Option[String],
    toBlock: Option[String],
    fromAddress: Option[String],
    toAddress: Option[String],
    category: Set[TokenCategory],
    withMetadata: Option[Boolean],
  ): F[List[Transfer]]
}
