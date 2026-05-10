package org.github.insider.realtime.traders

final case class TraderConfig(
  enabled: Boolean,
  recentScoreChangesBlocksLength: Int,
  minUsdForSingleMarket: BigDecimal,
  maxTotalBalancePercentForSingleMarket: Int,
  longScoreDrawdownPercentThreshold: BigDecimal,
  shortScoreDrawdownPercentThreshold: BigDecimal,
)
