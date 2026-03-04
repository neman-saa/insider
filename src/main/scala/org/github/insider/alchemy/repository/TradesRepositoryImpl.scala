package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.{Side, Trade}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import doobie.syntax.all._
import org.github.insider.alchemy.domain.User
import org.github.insider.alchemy.repository.codec._

class TradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends TradesRepository[F] {

  override def insert(trades: NonEmptyList[Trade]): F[Int] =
    Update[Trade](
      """
      |INSERT INTO trades_v2 (maker_address, token_id, side, amount, total_price, block_num, tx_hash, tx_index, block_timestamp)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      |""".stripMargin
    ).updateMany(trades).transact(transactor)

  override def leaderboard: F[List[User]] =
    sql"""
      |select
      |    maker_address,
      |    sum(tupled_data.1) as new_money,
      |    sum(tupled_data.2) as trade_profit,
      |    sum(tupled_data.3 * l_price) as remained_tokens
      |from (
      |    select
      |        maker_address,
      |        token_id,
      |        arrayFold(
      |            (s, x) ->
      |                tuple(
      |                    if(
      |                        x.3 = 'BUY' AND s.2 < x.5,
      |                        s.1 + (x.5 - s.2),
      |                        s.1
      |                    ),
      |                    if(
      |                        x.3 = 'BUY',
      |                        if(s.2 >= x.5, s.2 - x.5, 0),
      |                        s.2 + x.5
      |                    ),
      |                    if(
      |                        x.3 = 'BUY',
      |                        s.3 + x.4,
      |                        s.3 - x.4
      |                    )
      |                ),
      |            arraySort(t -> (t.1, t.2), data),
      |            tuple(0.0, 0.0, 0.0)
      |        ) as tupled_data,
      |        tokens.last_price as l_price
      |    from agg_trades final join tokens on tokens.id = token_id
      |) where sum(tupled_data.2) + sum(tupled_data.3 * l_price) - sum(tupled_data.1) > 50000
      |   order by (sum(tupled_data.2) + sum(tupled_data.3 * l_price)) / sum(tupled_data.1)
      |
      |""".query[User].to[List].transact(transactor)

  override def getLatestBlock: F[Long] =
    fr"""
      |SELECT ifNull(max(block_num), 51500000) FROM trades_v2
      |"""
      .stripMargin
      .query[Long]
      .unique
      .transact(transactor)
}

object TradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[TradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new TradesRepositoryImpl[F](transactor, logger))
}
