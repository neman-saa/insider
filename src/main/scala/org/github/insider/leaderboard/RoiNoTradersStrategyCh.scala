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
        |WITH
        |    (
        |        SELECT block_timestamp
        |        FROM trades_simulations
        |        WHERE block_num = $block
        |        LIMIT 1
        |    ) AS block_ts
        |SELECT
        |    maker_address,
        |    least(3, income / total_price_sum) AS roi,
        |    number_of_events,
        |    (income / total_price_sum) * log2(number_of_events) AS score,
        |    buy_total / trades_count AS avg_buy
        |FROM
        |(
        |    SELECT
        |        trades.maker_address AS maker_address,
        |        (
        |            sumIf(trades.amount / 1000000, side = 'BUY' AND trades.last_price = 1) -
        |            sumIf(trades.amount / 1000000, side = 'SELL' AND trades.last_price = 1)
        |        ) AS income,
        |        (
        |            sumIf(total_price, side = 'BUY') -
        |            sumIf(total_price, side = 'SELL')
        |        ) AS total_price_sum,
        |        count(DISTINCT trades.market_id) AS number_of_events,
        |        sumIf(total_price, side = 'BUY') AS buy_total,
        |        count() AS trades_count,
        |        max(block_timestamp) AS last_block_ts,
        |        countIf(side = 'BUY') AS buy_count
        |    FROM trades_simulations AS trades
        |    INNER JOIN tokens t ON trades.token_id = t.id
        |    INNER JOIN markets m ON m.id = t.market_id
        |    INNER JOIN events e ON m.event_id = e.id
        |    WHERE block_num < $block
        |    GROUP BY trades.maker_address
        |)
        |WHERE
        |    total_price_sum > 1000
        |    AND last_block_ts > block_ts - INTERVAL 10 DAY
        |    AND buy_count / trades_count > 0.8
        |    AND total_price_sum != 0
        |ORDER BY score DESC
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
