package org.github.insider.alchemy.repository

import cats.Parallel
import cats.effect.Async
import doobie.{Meta, Transactor}
import org.github.insider.polymarket.domain.{Side, Trade}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import doobie.syntax.all._
import cats.syntax.all._
import org.github.insider.polymarket.domain.Side.{Buy, Sell}

import java.util.UUID

class TradesRepositoryImpl[F[_]: Async: Parallel](transactor: Transactor[F], logger: Logger[F]) extends TradesRepository[F] {

  implicit val sideMeta: Meta[Side] = Meta[String].timap[Side]{
    case "BUY" => Buy
    case "SELL" => Sell
  }{
    case Buy => "BUY"
    case Sell => "SELL"
  }
  override def insert(trades: List[Trade]): F[Long] =
    trades.parTraverse{ trade =>
      sql"""
        INSERT INTO trades (id, side, maker_address, amount, total_price, created_at, token, hash)
        VALUES (
          ${UUID.randomUUID().toString},
          ${trade.side},
          ${trade.makerAddress},
          ${trade.size},
          ${trade.totalPrice},
          ${trade.timestamp},
          ${trade.assetId},
          ${trade.hash}
        )
      """.update.run.transact(transactor)
    }.map(_.sum)

}

object TradesRepositoryImpl {
  def of[F[_]: Async: Parallel](transactor: Transactor[F]): F[TradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new TradesRepositoryImpl[F](transactor, logger))
}
