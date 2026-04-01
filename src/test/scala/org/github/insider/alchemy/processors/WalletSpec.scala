package org.github.insider.alchemy.processors

import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.{HexAddress, LeaderboardEntry}
import org.github.insider.leaderboard.RoiNoTradersStrategyCh.RoiNoTradersEntry
import org.github.insider.polymarket.domain.Side
import org.github.insider.simulations.{SimulationConfig, SimulationTrade, Wallet}
import weaver.SimpleIOSuite

object WalletSpec extends SimpleIOSuite {
  import java.time.Instant

  val config =
    SimulationConfig(
      blocksProcessingBatchSize = 1000,
      leaderboardLimit          = 1000,
      leaderboardBlocksLifetime = 11000,
      initialWalletBalance      = 1000,
      minWalletBlocksLifetime   = 60_000,
      maxWalletBlocksLifetime   = 65_000,
      maxTemporaryWalletsInPool = 3,
      extraBuyPerCents          = 2,
      allowedPerCentsPerUser    = 10
    )
  val address = HexAddress("0xabc123abc123abc123abc123abc123abc123abcd")
  val leaderboardEntry = RoiNoTradersEntry(
    makerAddress          = address,
    totalLeaderboardSize  = 1,
    totalLeaderboardScore = 100,
    roi                   = 2,
    rank                  = 1,
    score                 = 100,
    numberOfEvents        = 100,
    avgBuy                = 5,
  )
  val leaderboard: Map[HexAddress, RoiNoTradersEntry] = Map(address -> leaderboardEntry)

  val testTrades: List[SimulationTrade] = List(
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:00:00Z")),
      blockNum       = 1001,
      txIndex        = 0,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("12.5"),
      totalPrice     = BigDecimal("10.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:05:00Z"),
      startTime      = Instant.parse("2026-03-01T09:59:00Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:01:00Z")),
      blockNum       = 1002,
      txIndex        = 1,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Sell,
      amount         = BigDecimal("4.8"),
      totalPrice     = BigDecimal("4.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:06:00Z"),
      startTime      = Instant.parse("2026-03-01T10:00:30Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:02:00Z")),
      blockNum       = 1002,
      txIndex        = 2,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("8.4"),
      totalPrice     = BigDecimal("7.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:07:00Z"),
      startTime      = Instant.parse("2026-03-01T10:01:10Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:03:00Z")),
      blockNum       = 1002,
      txIndex        = 3,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("3.3"),
      totalPrice     = BigDecimal("3.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:08:00Z"),
      startTime      = Instant.parse("2026-03-01T10:02:20Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:04:00Z")),
      blockNum       = 1005,
      txIndex        = 0,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Sell,
      amount         = BigDecimal("5.75"),
      totalPrice     = BigDecimal("5.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:09:00Z"),
      startTime      = Instant.parse("2026-03-01T10:03:10Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:05:00Z")),
      blockNum       = 1006,
      txIndex        = 1,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("14.4"),
      totalPrice     = BigDecimal("12.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:10:00Z"),
      startTime      = Instant.parse("2026-03-01T10:04:15Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:06:00Z")),
      blockNum       = 1007,
      txIndex        = 0,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Sell,
      amount         = BigDecimal("6.9"),
      totalPrice     = BigDecimal("6.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:11:00Z"),
      startTime      = Instant.parse("2026-03-01T10:05:05Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:07:00Z")),
      blockNum       = 1008,
      txIndex        = 3,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("9.2"),
      totalPrice     = BigDecimal("8.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:12:00Z"),
      startTime      = Instant.parse("2026-03-01T10:06:40Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:08:00Z")),
      blockNum       = 1009,
      txIndex        = 0,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Sell,
      amount         = BigDecimal("9.9"),
      totalPrice     = BigDecimal("9.0"),
      lastPrice      = 1,
      closedTime     = Instant.parse("2026-03-01T10:13:00Z"),
      startTime      = Instant.parse("2026-03-01T10:07:25Z"),
      marketId       = "MARKET-1"
    ),
    SimulationTrade(
      blockTimestamp = Some(Instant.parse("2026-03-01T10:09:00Z")),
      blockNum       = 1010,
      txIndex        = 2,
      makerAddress   = "0xabc123abc123abc123abc123abc123abc123abcd",
      tokenId        = "TOKEN-1",
      side           = Side.Buy,
      amount         = BigDecimal("16.5"),
      totalPrice     = BigDecimal("15.0"),
      lastPrice      = 110,
      closedTime     = Instant.parse("2026-03-01T10:14:00Z"),
      startTime      = Instant.parse("2026-03-01T10:08:10Z"),
      marketId       = "MARKET-1"
    )
  )

  val wallet = Wallet(
    initialBalance  = 1000,
    lockedBalance   = 0,
    freeBalance     = 1000,
    id              = "test-wallet",
    tokens          = Map.empty,
    activeFromBlock = 1000,
    activeToBlock   = Some(1010)
  )

  def processTrades(
    trades: List[SimulationTrade],
    wallet: Wallet,
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry],
  )(config: SimulationConfig): Wallet = {
    val tradesMatchesLeaderboard: List[(SimulationTrade, AdvancedLeaderboardEntry)] =
      trades.flatMap(trade => leaderboard.get(HexAddress(trade.makerAddress)).map(entry => (trade, entry)))
    val updatedWallet: Wallet =
      tradesMatchesLeaderboard.foldLeft[Wallet](wallet) {
        case (currentWallet, (trade, leaderboardEntry)) =>
          trade.side match {
            case Side.Buy =>
              val maybeUpdatedWallet =
                currentWallet.copyBuy(
                  tokenId          = trade.tokenId,
                  leader           = HexAddress(trade.makerAddress),
                  amount           = trade.amount,
                  totalPrice       = trade.totalPrice,
                  leaderboardEntry = leaderboardEntry,
                )(config)
              maybeUpdatedWallet.getOrElse(currentWallet)
            case Side.Sell =>
              val maybeUpdatedWallet =
                currentWallet.copySell(
                  tokenId    = trade.tokenId,
                  leader     = HexAddress(trade.makerAddress),
                  amount     = trade.amount,
                  totalPrice = trade.totalPrice,
                )

              maybeUpdatedWallet.getOrElse(currentWallet)
          }
      }

    updatedWallet
  }

  pureTest("wallet should be processed properly") {

    val processedWallet: Wallet = processTrades(testTrades, wallet, leaderboard)(config)
    val allowedPrice = processedWallet.tokens.head._2.allowedTotalPrice
    val ourFirstPrice = processedWallet.tokens.head._2.ourFirstPrice
    val leaderFirstBuy = processedWallet.tokens.head._2.leaderFirstBuy
    val freeBalance = processedWallet.freeBalance
    val lockBalance = processedWallet.lockedBalance
    val expectedTotalPrice = testTrades.foldLeft(BigDecimal(0)) {
      case (balance, trade) =>
        val sign = trade.side match {
          case Side.Buy => 1;
          case Side.Sell => -1
        }
        balance + ourFirstPrice * trade.totalPrice * sign / leaderFirstBuy
    }
    val expectedPutIn = expectedTotalPrice min allowedPrice
    val (_, expectedAmount) = testTrades.foldLeft((BigDecimal(0), BigDecimal(0))) {
      case ((balance, amount), trade) =>
        val sign = trade.side match {
          case Side.Buy => 1;
          case Side.Sell => -1
        }
        val newBalance = balance + ourFirstPrice * trade.totalPrice * sign / leaderFirstBuy
        val singleTokenPrice = trade.totalPrice / trade.amount + 0.01 * sign
        val putIn = balance min allowedPrice
        val newPutIn = newBalance min allowedPrice
        val newAmount = amount + (newPutIn - putIn) / singleTokenPrice
        (newBalance, newAmount)
    }

    expect(processedWallet.tokens.size == 1) and
      expect(allowedPrice == 100) and
      expect(expectedTotalPrice == processedWallet.tokens.head._2.ourTotalPrice) and
      expect(expectedPutIn == processedWallet.tokens.head._2.ourTotalPricePutIn) and
      expect(expectedAmount == processedWallet.tokens.head._2.ourAmount) and
      expect(ourFirstPrice == 40) and
      expect(leaderFirstBuy == 10) and
      expect(lockBalance == 0) and
      expect(freeBalance == 900)
  }
}
