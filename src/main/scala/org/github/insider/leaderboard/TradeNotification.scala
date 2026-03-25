package org.github.insider.leaderboard

import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
import org.github.insider.polymarket.domain.{Event, Trade}

final case class TradeNotification(
  trade: Trade,
  leaderboardEntries: List[(LeaderboardKeyName, LeaderboardEntry)],
  event: Event,
  followers: Set[String],
)
