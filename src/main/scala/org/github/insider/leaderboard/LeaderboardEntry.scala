package org.github.insider.leaderboard

trait LeaderboardEntry {
  def makerAddress: HexAddress

  def totalLeaderboardSize: Int
  def rank: Int

  def prettyPrint: String

}

object LeaderboardEntry {
  trait SimpleLeaderboardEntry extends LeaderboardEntry
  trait AdvancedLeaderboardEntry extends LeaderboardEntry {
    def score: BigDecimal
    def numberOfEvents: Int
    def avgBuy: BigDecimal
    def totalLeaderboardScore: BigDecimal
  }
}
