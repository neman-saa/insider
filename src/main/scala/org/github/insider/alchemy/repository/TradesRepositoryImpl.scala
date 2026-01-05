package org.github.insider.alchemy.repository

import cats.effect.Async
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import doobie.syntax.all._

class TradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends TradesRepository[F] {

  override def insert(trades: List[Trade]): F[Int] =
    Update[Trade](
      """
        |INSERT INTO trades (maker_address, token, amount, total_price, created_at, polygon_tx_hash, side)
        |VALUES (?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    ).updateMany(trades).transact(transactor)
}

object TradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[TradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new TradesRepositoryImpl[F](transactor, logger))
}
