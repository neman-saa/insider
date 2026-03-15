package org.github.insider.leaderboard

trait LeaderboardEntry {
  def makerAddress: HexAddress

  def totalLeaderboardSize: Int
  def rank: Int

  def prettyPrint: String

  def score: BigDecimal
}
