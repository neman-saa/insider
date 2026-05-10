package org.github.insider.realtime.traders

final case class TraderConfig(
  enabled: Boolean,
  recentScoreChangesBlocksLength: Int,
  minUsdForSingleMarket: BigDecimal,
  maxTotalBalancePercentForSingleMarket: BigDecimal,
  longScoreDrawdownPercentThreshold: BigDecimal,
  shortScoreDrawdownPercentThreshold: BigDecimal,
)
