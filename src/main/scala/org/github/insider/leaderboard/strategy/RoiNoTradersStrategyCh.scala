package org.github.insider.leaderboard.strategy

import cats.effect.Sync
import doobie.Transactor
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.leaderboard.strategy.RoiNoTradersStrategyCh.RoiNoTradersEntry
import org.github.insider.leaderboard.{HexAddress, LeaderboardStrategy}

private class RoiNoTradersStrategyCh[F[_]: Sync](transactor: Transactor[F])
    extends LeaderboardStrategy[F, AdvancedLeaderboardEntry] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("roi-leaderboard-with-no-traders")

  override def load(block: Long, limit: Int): F[Map[HexAddress, AdvancedLeaderboardEntry]] =
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
          .toMap[HexAddress, AdvancedLeaderboardEntry]
      )
      .transact(transactor)

  private def query(limit: Int, block: Long): Fragment =
    fr"""
      |WITH
      |(
      |   SELECT block_timestamp
      |   FROM trades
      |   WHERE block_num = $block
      |   LIMIT 1
      |) AS block_ts
      |SELECT
      |    trades.maker_address,
      |    least(
      |        (
      |            sumIf(amount / 1000000, side = 'BUY' AND last_price = 1) -
      |            sumIf(amount / 1000000, side = 'SELL' AND last_price = 1)
      |        ) /
      |        nullIf(
      |            sumIf(total_price, side = 'BUY') -
      |            sumIf(total_price, side = 'SELL'),
      |            0
      |        ),
      |        3
      |    ) AS roi,
      |    count(DISTINCT market_id) AS number_of_events,
      |    roi * log2(number_of_events) AS score,
      |    sumIf(total_price, side = 'BUY') / nullIf(countIf(side = 'BUY'), 0) AS avg_buy
      |FROM trades
      |JOIN tokens ts ON trades.token_id = ts.id
      |JOIN markets ON ts.market_id = markets.id
      |WHERE block_num < $block
      |	AND total_price / amount * 1000000 > 0.02
      |  	AND total_price / amount * 1000000 < 0.98
      |  	AND block_timestamp < markets.end_date
      |  	AND markets.end_date > '2000-12-22 01:23:00'
      |GROUP BY trades.maker_address
      |HAVING
      |    max(block_timestamp) > block_ts - INTERVAL 10 DAY
      |    AND (
      |        sumIf(total_price, side = 'BUY') -
      |        sumIf(total_price, side = 'SELL')
      |    ) > 1000
      |    AND countIf(side = 'BUY') / count(*) > 0.8
      |    AND roi > 0.95
      |ORDER BY score DESC
      |LIMIT $limit
      """.stripMargin
}

object RoiNoTradersStrategyCh {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F, AdvancedLeaderboardEntry] =
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
  ) extends AdvancedLeaderboardEntry {
    override def prettyPrint: String =
      s"rank - $rank/$totalLeaderboardSize, roi - $roi, number of events - $numberOfEvents, avg buy - $avgBuy"
  }
}
