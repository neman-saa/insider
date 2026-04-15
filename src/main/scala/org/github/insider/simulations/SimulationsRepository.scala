package org.github.insider.simulations

import cats.data.NonEmptyList
import cats.effect.Sync
import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragment.Fragment
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class SimulationsRepository[F[_]: Sync](transactor: Transactor[F], logger: Logger[F]) {

  def getHistoricalTrades(from: Int, to: Int): F[List[SimulationTrade]] =
    sql"""
         |select * from trades_simulations
         |where block_num >= $from
         |and block_num <= $to
         |and total_price / amount * 1000000 > 0.02 and total_price / amount * 1000000 < 0.98
         |and block_timestamp < end_time
         |and end_time > '2000-12-22 01:23:00'
         |order by (block_num, tx_index)
         |"""
      .stripMargin
      .query[SimulationTrade]
      .to[List]
      .transact(transactor)

  def timestampByBlockNum(blockNum: Int): F[Instant] =
    sql"select min(block_timestamp) from trades_simulations where block_num > $blockNum"
      .query[Instant]
      .unique
      .transact(transactor)

  def insertWallets(wallets: NonEmptyList[Wallet]): F[Unit] =
    createQuery(wallets.toList)
      .update
      .run
      .transact(transactor)
      .flatMap(rows => logger.info(s"Inserted $rows trades in 'wallets' table"))

  private def createQuery(wallets: List[Wallet]): Fragment = {
    val insert =
      fr"""
          |INSERT INTO wallets2 (
          |   wallet_id, 
          |   initial_balance, 
          |   final_balance,
          |   active_from_block,
          |   active_to_block, 
          |   insert_time
          |) VALUES
          |""".stripMargin
    val values = wallets
      .map(wallet => fr"""
                        |(
                        |   ${wallet.id},
                        |   ${wallet.initialBalance},
                        |   ${wallet.currentBalance},
                        |   ${wallet.activeFromBlock},
                        |   ${wallet.activeToBlock.getOrElse(-1)},
                        |   now()
                        |)
                        |""".stripMargin)
      .reduce(_ ++ _)

    insert ++ values
  }
}

object SimulationsRepository {
  def of[F[_]: Sync](transactor: Transactor[F]): F[SimulationsRepository[F]] =
    Slf4jLogger.create[F].map(logger => new SimulationsRepository[F](transactor, logger))
}
