package org.github.insider.leaderboard

trait LeaderboardEntry {
  def makerAddress: HexAddress

  def totalLeaderboardSize: Int
  def totalLeaderboardScore: BigDecimal
  def rank: Int

  def prettyPrint: String

  def score: BigDecimal
  def numberOfEvents: Int
  def avgBuy: BigDecimal
}
