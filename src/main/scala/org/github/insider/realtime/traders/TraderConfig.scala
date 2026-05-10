package org.github.insider.realtime.traders

final case class TraderConfig(
  enabled: Boolean,
  minUsdForSingleMarket: BigDecimal,
  maxTotalBalancePercentForSingleMarket: Int,
  longScoreDrawdownPercentThreshold: BigDecimal,
  shortScoreDrawdownPercentThreshold: BigDecimal,
)
