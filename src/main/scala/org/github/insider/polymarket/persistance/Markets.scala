package org.github.insider.polymarket.persistance

import org.github.insider.polymarket.domain.{Market, Trade}

import scala.collection.immutable.HashMap

trait Markets[F[_]] {

  def addMarket(market: Market, map: HashMap[(String, String), (BigDecimal, BigDecimal)]): F[Unit]

}
