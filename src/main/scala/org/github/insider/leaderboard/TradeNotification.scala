package org.github.insider.leaderboard

import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.polymarket.domain.Trade

final case class TradeNotification(
  trade: Trade,
  leaderboardEntries: List[(LeaderboardKeyName, LeaderboardEntry)],
)
