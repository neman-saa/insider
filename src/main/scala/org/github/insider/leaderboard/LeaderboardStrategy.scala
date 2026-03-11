package org.github.insider.leaderboard

import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName

trait LeaderboardStrategy[F[_]] {

  def key: LeaderboardKeyName

  def load: F[Map[HexAddress, LeaderboardEntry]]

}

object LeaderboardStrategy {
  final case class LeaderboardKeyName(value: String) extends AnyVal
}
