package org.github.insider.leaderboard

import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName

trait LeaderboardStrategy[F[_], Entry <: LeaderboardEntry] {

  def key: LeaderboardKeyName

  def load(fromBlock: Long, limit: Int): F[Map[HexAddress, Entry]]

}

object LeaderboardStrategy {
  final case class LeaderboardKeyName(value: String) extends AnyVal
}
