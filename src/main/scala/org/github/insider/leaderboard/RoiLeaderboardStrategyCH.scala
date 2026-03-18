package org.github.insider.leaderboard

import cats.effect.Sync
import doobie.Transactor
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragment.Fragment
import org.github.insider.leaderboard.RoiLeaderboardStrategyCH.RoiLeaderboardEntry

import java.time.{Instant, ZoneOffset}

private class RoiLeaderboardStrategyCH[F[_]: Sync](transactor: Transactor[F]) extends LeaderboardStrategy[F] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("Total Profit Leaderboard")

  override def load(currentDate: Instant = Instant.now(), limit: Int): F[Map[HexAddress, LeaderboardEntry]] =
    queryWithDate(currentDate, limit)
      .query[(String, BigDecimal, BigDecimal, BigDecimal, Int)]
      .to[List]
      .map(list =>
        list
          .zipWithIndex
          .map {
            case ((makerAddress, newMoney, profit, score, nEvents), index) =>
              val address = HexAddress(makerAddress)
              (address, RoiLeaderboardEntry(address, index + 1, newMoney, profit, nEvents, list.size, score))
          }
          .toMap[HexAddress, LeaderboardEntry]
      )
      .transact(transactor)

  private def queryWithDate(date: Instant, limit: Int): Fragment =
    fr"""
        |SELECT
        |    maker_address,
        |    sum(new_money) AS all_new_money,
        |    sum(trade_profit) + sum(remained_tokens * last_price) AS all_profit,
        |    count(DISTINCT market_id) AS markets_count,
        |    least(
        |        3.0,
        |        (sum(trade_profit) + sum(remained_tokens * last_price)) / sum(new_money)
        |    ) * sqrt(count(DISTINCT market_id) * sum(new_money)) AS score
        |FROM
        |(
        |    SELECT
        |        maker_address,
        |        token_id,
        |        tupled_data.1 AS new_money,
        |        tupled_data.2 AS trade_profit,
        |        tupled_data.3 / 1000000.0 AS remained_tokens,
        |        tokens.last_price AS last_price,
        |        markets.startDate AS market_start_date,
        |        arrayMax(t -> t.1, data) AS latest_block,
        |        markets.id AS market_id
        |    FROM
        |    (
        |        SELECT
        |            maker_address,
        |            token_id,
        |            data,
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
        |        markets.startDate < $date
        |        AND (tokens.last_price = 0 OR tokens.last_price = 1)
        |) s
        |GROUP BY maker_address
        |HAVING
        |    sum(new_money) > 1000
        |    AND max(market_start_date) > $date - INTERVAL 270 DAY
        |ORDER BY score DESC
        |LIMIT 1000
      """
}

object RoiLeaderboardStrategyCH {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F] =
    new RoiLeaderboardStrategyCH[F](transactor)

  final case class RoiLeaderboardEntry(
    makerAddress: HexAddress,
    rank: Int,
    newMoney: BigDecimal,
    profit: BigDecimal,
    eventsNumber: Int,
    totalLeaderboardSize: Int,
    score: BigDecimal
  ) extends LeaderboardEntry {
    override def prettyPrint: String =
      s"""
         |rank - $rank/$totalLeaderboardSize,
         |new money - $newMoney,
         |profit - $profit,
         |roi - ${profit / newMoney},
         |events number - $eventsNumber""".stripMargin
  }
}
