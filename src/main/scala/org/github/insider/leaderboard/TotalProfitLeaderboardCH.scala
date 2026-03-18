package org.github.insider.leaderboard

import cats.effect.{IO, Sync}
import doobie.Transactor
import doobie.util.fragment.Fragment
import doobie.implicits._
import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.leaderboard.TotalProfitLeaderboardCH.TotalProfitLeaderboardEntry

private class TotalProfitLeaderboardCH[F[_]: Sync](transactor: Transactor[F]) extends LeaderboardStrategy[F] {

  override def key: LeaderboardKeyName = LeaderboardKeyName("Total Profit Leaderboard")

  override def load(limit: Int): F[Map[HexAddress, LeaderboardEntry]] =
    query
      .query[(String, BigDecimal, BigDecimal)]
      .to[List]
      .map(list =>
        list
          .zipWithIndex
          .map {
            case ((makerAddress, newMoney, profit), index) =>
              val address = HexAddress(makerAddress)
              (address, TotalProfitLeaderboardEntry(address, list.size, index + 1, newMoney, profit))
          }
          .toMap[HexAddress, LeaderboardEntry]
      )
      .transact(transactor)

  private def query: Fragment = ???
}

object TotalProfitLeaderboardCH {

  def apply[F[_]: Sync](transactor: Transactor[F]): LeaderboardStrategy[F] =
    new TotalProfitLeaderboardCH[F](transactor)

  final case class TotalProfitLeaderboardEntry(
    makerAddress: HexAddress,
    totalLeaderboardSize: Int,
    rank: Int,
    newMoney: BigDecimal,
    profit: BigDecimal,
  ) extends LeaderboardEntry {
    override def prettyPrint: String =
      s"rank - $rank/$totalLeaderboardSize, new money - $newMoney, profit - $profit"
  }
}
