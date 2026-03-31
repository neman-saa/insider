package org.github.insider.leaderboard

import cats.effect.Sync
import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragment.Fragment
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.leaderboard.WinRateLeaderboardStrategyCH.WinRateLeaderboardEntry

private class WinRateLeaderboardStrategyCH[F[_]: Sync](transactor: Transactor[F]) extends LeaderboardStrategy[F] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("Win Rate Leaderboard")

  override def load(fromBlock: Long, limit: Int): F[Map[HexAddress, LeaderboardEntry]] =
    query(limit)
      .query[(String, Int, Int)]
      .to[List]
      .map(list =>
        list
          .zipWithIndex
          .map {
            case ((makerAddress, win, lose), index) =>
              val address = HexAddress(makerAddress)
              (address, WinRateLeaderboardEntry(address, list.size, index + 1, win, lose, 0, 0, 0, 0))
          }
          .toMap[HexAddress, LeaderboardEntry]
      )
      .transact(transactor)

  private def query(limit: Int): Fragment =
    fr"""
        |SELECT
        |	maker_address,
        |	countIf(last_price = 1) AS win_count,
        |	countIf(last_price = 0) AS lose_count
        |   count(distinct tokens.market_id) as number_events,
        |
        |FROM (
        |    SELECT
        |        maker_address,
        |        token_id,
        |        tokens.last_price AS last_price
        |    FROM agg_trades FINAL JOIN tokens ON tokens.id = token_id
        |    WHERE last_price = 0 OR last_price = 1
        |)
        |GROUP BY maker_address
        |HAVING win_count / (win_count + lose_count) > 0.7
        |ORDER BY win_count DESC
        |LIMIT $limit
        """.stripMargin // Starting range 83725101 - 83725200
}

object WinRateLeaderboardStrategyCH {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F] =
    new WinRateLeaderboardStrategyCH[F](transactor)

  final case class WinRateLeaderboardEntry(
    makerAddress: HexAddress,
    totalLeaderboardSize: Int,
    rank: Int,
    win: Int,
    lose: Int,
    score: BigDecimal,
    avgBuy: BigDecimal,
    numberOfEvents: Int,
    totalLeaderboardScore: BigDecimal
  ) extends LeaderboardEntry {
    override def prettyPrint: String =
      s"rank - $rank/$totalLeaderboardSize, win - $win, lose - $lose"
  }
}
