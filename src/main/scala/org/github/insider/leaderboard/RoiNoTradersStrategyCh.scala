package org.github.insider.leaderboard

import cats.effect.Sync
import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragment.Fragment
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.leaderboard.RoiNoTradersStrategyCh.RoiNoTradersEntry

private class RoiNoTradersStrategyCh[F[_]: Sync](transactor: Transactor[F]) extends LeaderboardStrategy[F] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("roi-leaderboard-with-no-traders")

  override def load(block: Long, limit: Int): F[Map[HexAddress, LeaderboardEntry]] =
    query(limit, block)
      .query[(String, BigDecimal, Int, BigDecimal, BigDecimal)]
      .to[List]
      .map(list =>
        list
          .zipWithIndex
          .map {
            case ((makerAddress, roi, nEvents, score, avgBuy), index) =>
              val address = HexAddress(makerAddress)
              (address, RoiNoTradersEntry(address, list.length, index, roi, score, nEvents, avgBuy, list.map(_._4).sum))
          }
          .toMap[HexAddress, LeaderboardEntry]
      )
      .transact(transactor)

  private def query(limit: Int, block: Long): Fragment =
    fr"""
        |WITH (
        |   SELECT block_timestamp
        |   FROM trades_simulations
        |   WHERE block_num = $block
        |   LIMIT 1
        |) AS block_ts
        |SELECT
        |    maker_address AS maker_address,
        |    least((
        |        sumIf(amount / 1000000, side = 'BUY' AND last_price = 1) -
        |        sumIf(amount / 1000000, side = 'SELL' AND last_price = 1)
        |    ) /
        |    (
        |        sumIf(total_price, side = 'BUY') -
        |        sumIf(total_price, side = 'SELL')
        |    ), 3) AS roi,
        |    count(DISTINCT market_id) AS number_of_events,
        |    roi * log2(number_of_events) as score,
        |    sumIf(total_price, side = 'BUY') / countIf(side = 'BUY') AS avg_buy
        |FROM trades_simulations AS trades
        |WHERE block_num < $block
        |GROUP BY trades.maker_address
        |HAVING
        |   max(block_timestamp) > block_ts - INTERVAL 10 DAY AND
        |   (
        |     sumIf(total_price, side = 'BUY') -
        |     sumIf(total_price, side = 'SELL')
        |   ) > 1000 AND
        |   countIf(side = 'BUY') / count(*) > 0.8 AND
        |   (
        |     sumIf(total_price, side = 'BUY') -
        |     sumIf(total_price, side = 'SELL')
        |   ) != 0
        |LIMIT $limit
      """.stripMargin
}

object RoiNoTradersStrategyCh {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F] =
    new RoiNoTradersStrategyCh[F](transactor)

  final case class RoiNoTradersEntry(
    makerAddress: HexAddress,
    totalLeaderboardSize: Int,
    rank: Int,
    roi: BigDecimal,
    score: BigDecimal,
    numberOfEvents: Int,
    avgBuy: BigDecimal,
    totalLeaderboardScore: BigDecimal
  ) extends LeaderboardEntry {
    override def prettyPrint: String =
      s"rank - $rank/$totalLeaderboardSize, roi - $roi, number of events - $numberOfEvents, avg buy - $avgBuy"
  }
}
