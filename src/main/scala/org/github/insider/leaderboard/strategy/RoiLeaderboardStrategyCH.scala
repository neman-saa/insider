package org.github.insider.leaderboard.strategy

import cats.effect.Sync
import doobie.Transactor
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.leaderboard.strategy.RoiLeaderboardStrategyCH.RoiLeaderboardEntry
import org.github.insider.leaderboard.{HexAddress, LeaderboardStrategy}

import java.time.Instant

private class RoiLeaderboardStrategyCH[F[_]: Sync](transactor: Transactor[F])
    extends LeaderboardStrategy[F, AdvancedLeaderboardEntry] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("Total Profit Leaderboard")

  override def load(block: Long, limit: Int): F[Map[HexAddress, AdvancedLeaderboardEntry]] =
    queryWithDate(block, limit)
      .query[(String, BigDecimal, BigDecimal, BigDecimal, Int, BigDecimal)]
      .to[List]
      .map(list =>
        list
          .zipWithIndex
          .map {
            case ((makerAddress, newMoney, profit, score, nEvents, avgBuy), index) =>
              val address = HexAddress(makerAddress)
              (
                address,
                RoiLeaderboardEntry(
                  address,
                  index + 1,
                  newMoney,
                  profit,
                  list.size,
                  score,
                  list.map(_._4).sum,
                  nEvents,
                  avgBuy
                )
              )
          }
          .toMap[HexAddress, AdvancedLeaderboardEntry]
      )
      .transact(transactor)

  private def queryWithDate(block: Long, limit: Int): Fragment =
    fr"""
        |WITH
        |    (
        |        SELECT max(block_timestamp)
        |        FROM trades
        |        WHERE block_num = $block
        |    ) AS block_ts
        |SELECT
        |    maker_address,
        |    sum(new_money) AS all_new_money,
        |    sum(trade_profit) + sum(remained_tokens * last_price) AS all_profit,
        |    count(DISTINCT market_id) AS markets_count,
        |    least(
        |        3.0,
        |        (sum(trade_profit) + sum(remained_tokens * last_price)) / sum(new_money)
        |    ) * sqrt(count(DISTINCT market_id) * sum(new_money)) AS score,
        |    max(last_activity_timestamp) AS last_activity_timestamp
        |FROM
        |(
        |    SELECT
        |        maker_address,
        |        token_id,
        |        markets.id AS market_id,
        |        tupled_data.1 AS new_money,
        |        tupled_data.2 AS trade_profit,
        |        tupled_data.3 / 1000000.0 AS remained_tokens,
        |        tokens.last_price AS last_price,
        |        last_activity_timestamp
        |    FROM
        |    (
        |        SELECT
        |            maker_address,
        |            token_id,
        |            data,
        |            last_activity_timestamp,
        |            arrayFold(
        |                (s, x) ->
        |                    tuple(
        |                        if(
        |                            x.3 = 'BUY' AND s.2 < x.5,
        |                            s.1 + (x.5 - s.2),
        |                            s.1
        |                        ),
        |                        if(
        |                            x.3 = 'BUY',
        |                            if(s.2 >= x.5, s.2 - x.5, 0.0),
        |                            s.2 + x.5
        |                        ),
        |                        if(
        |                            x.3 = 'BUY',
        |                            s.3 + x.4,
        |                            s.3 - x.4
        |                        )
        |                    ),
        |                arraySort(t -> (t.1, t.2), data),
        |                tuple(0.0, 0.0, 0.0)
        |            ) AS tupled_data
        |        FROM agg_trades
        |        FINAL
        |    ) t
        |    INNER JOIN tokens ON tokens.id = t.token_id
        |    INNER JOIN markets ON tokens.market_id = markets.id
        |    WHERE
        |        markets.start_date < block_ts
        |        AND (tokens.last_price = 0 OR tokens.last_price = 1)
        |) s
        |GROUP BY maker_address
        |HAVING
        |    sum(new_money) > 1000
        |    AND max(last_activity_timestamp) > block_ts - INTERVAL 270 DAY
        |ORDER BY score DESC
        |LIMIT $limit
    """
}

object RoiLeaderboardStrategyCH {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F, AdvancedLeaderboardEntry] =
    new RoiLeaderboardStrategyCH[F](transactor)

  final case class RoiLeaderboardEntry(
    makerAddress: HexAddress,
    rank: Int,
    newMoney: BigDecimal,
    profit: BigDecimal,
    totalLeaderboardSize: Int,
    score: BigDecimal,
    totalLeaderboardScore: BigDecimal,
    numberOfEvents: Int,
    avgBuy: BigDecimal
  ) extends AdvancedLeaderboardEntry {
    override def prettyPrint: String =
      s"""
         |rank - $rank/$totalLeaderboardSize,
         |new money - $newMoney,
         |profit - $profit,
         |roi - ${profit / newMoney},
         |events number - $numberOfEvents""".stripMargin
  }
}
