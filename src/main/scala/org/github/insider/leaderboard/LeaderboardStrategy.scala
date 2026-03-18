package org.github.insider.leaderboard

import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName

import java.time.Instant

trait LeaderboardStrategy[F[_]] {

  def key: LeaderboardKeyName

  def load(currentDate: Instant): F[Map[HexAddress, LeaderboardEntry]]

}

object LeaderboardStrategy {
  final case class LeaderboardKeyName(value: String) extends AnyVal
}
