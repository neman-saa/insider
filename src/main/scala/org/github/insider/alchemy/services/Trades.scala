package org.github.insider.alchemy.services

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.polymarket.domain.Trade

trait Trades[F[_]] {
  def getTradesFromBlock(transfers: List[AssetTransfer], ctfAddress: String, wtf: String): List[Trade]
  def writeTrades(trades: List[Trade]): F[Unit]
  def exec(transfers: List[AssetTransfer]): F[Unit]
}
