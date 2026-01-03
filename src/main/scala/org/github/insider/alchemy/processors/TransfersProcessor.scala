package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.polymarket.domain.Trade

trait TransfersProcessor {
  def extractTradesFrom(transfers: List[AssetTransfer]): List[Trade]
}
