package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.polymarket.domain.Trade

trait TransfersProcessor[F[_]] {
  def extractTradesFrom(transfersTo: List[AssetTransfer], transfersFrom: List[AssetTransfer]): F[List[Trade]]
}
