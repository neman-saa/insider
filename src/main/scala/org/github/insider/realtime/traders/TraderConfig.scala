package org.github.insider.realtime.traders

final case class TraderConfig(
  enabled: Boolean,
  recentScoreChangesBlocksLength: Int,
  minUsdForSingleMarket: BigDecimal,
  maxActiveMarketsCount: Int,
  longScoreDrawdownPercentThreshold: BigDecimal,
  shortScoreDrawdownPercentThreshold: BigDecimal,
  maxBuyPriceAddCents: BigDecimal,
)
