package org.github.insider.polymarket.client

import org.github.insider.polymarket.domain.Trade

trait TradesClient[F[_]]{
  def getTradesHistoryByMarket(conditionId: String): F[List[Trade]]
}
