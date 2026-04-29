package org.github.insider.realtime.wallets

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.polymarket.domain.{BuyOrderResult, Position, SellOrderResult}
import org.github.insider.realtime.tokens.{TokenInfo, TokensInfoRegistry, TokensInfoRepository}
import weaver.SimpleIOSuite

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WalletSpec extends SimpleIOSuite {

  private sealed trait Operation {
    def tokenId: String
  }

  private object Operation {
    final case class Sell(tokenId: String, shares: BigDecimal, minPrice: Option[BigDecimal]) extends Operation
    final case class Buy(tokenId: String, shares: BigDecimal, maxPrice: Option[BigDecimal])  extends Operation
  }

  private final case class WalletFixture(
    wallet: Wallet[IO],
    operationsR: Ref[IO, Vector[Operation]],
    balanceR: Ref[IO, BigDecimal],
    positionsR: Ref[IO, List[Position]],
  )

  private final class TestTradingClient(
    positionsR: Ref[IO, List[Position]],
    balanceR: Ref[IO, BigDecimal],
    operationsR: Ref[IO, Vector[Operation]],
    prices: Map[String, BigDecimal],
  ) extends TradingClient[IO] {

    override def buy(tokenId: String, amount: BigDecimal, maxPrice: Option[BigDecimal]): IO[BuyOrderResult] = {
      val price = maxPrice.getOrElse(prices(tokenId))

      for {
        balance <- balanceR.get
        cost = (amount * price).min(balance)
        bought = if (price > 0) cost / price else BigDecimal(0)
        _ <- positionsR.update { positions =>
          val currentSize = positions.find(_.asset == tokenId).fold(BigDecimal(0))(_.size)
          Position(tokenId, currentSize + bought) :: positions.filterNot(_.asset == tokenId)
        }
        _ <- balanceR.update(_ - cost)
        _ <- operationsR.update(_ :+ Operation.Buy(tokenId, bought, maxPrice))
      } yield BuyOrderResult(bought, cost)
    }

    override def sell(tokenId: String, shares: BigDecimal, minPrice: Option[BigDecimal]): IO[SellOrderResult] = {
      val price = minPrice.getOrElse(prices.getOrElse(tokenId, BigDecimal(0)))

      for {
        positions <- positionsR.get
        currentSize = positions.find(_.asset == tokenId).fold(BigDecimal(0))(_.size)
        sold       = shares.min(currentSize)
        totalPrice = sold * price
        _ <- positionsR.update { positions =>
          val updatedSize = currentSize - sold
          val withoutSold = positions.filterNot(_.asset == tokenId)

          if (updatedSize > 0) Position(tokenId, updatedSize) :: withoutSold
          else withoutSold
        }
        _ <- balanceR.update(_ + totalPrice)
        _ <- operationsR.update(_ :+ Operation.Sell(tokenId, sold, minPrice))
      } yield SellOrderResult(sold, totalPrice)
    }

    override def balance(): IO[BigDecimal] =
      balanceR.get

    override def positions(user: Option[String]): IO[List[Position]] =
      positionsR.get

    override def buyOrder(tokenId: String, amount: BigDecimal, price: BigDecimal): IO[Unit] =
      IO.unit

    override def sellOrder(tokenId: String, shares: BigDecimal, price: BigDecimal): IO[Unit] =
      IO.unit
  }

  private def walletWith(
    infos: Map[String, (BigDecimal, BigDecimal)],
    positions: List[Position],
    balance: BigDecimal,
    marketsAmount: Int,
    thresholdPercent: Int = 10,
    spreadPercent: Int = 1,
  ): IO[WalletFixture] =
    for {
      now         <- IO.realTimeInstant
      repository  = testRepository(infos, now)
      registry   <- TokensInfoRegistry.withInit[IO](repository, cleanUpPeriod = 1.hour, secondsToSellBeforeResolve = 60)
      positionsR <- Ref.of[IO, List[Position]](positions)
      balanceR   <- Ref.of[IO, BigDecimal](balance)
      operationsR <- Ref.of[IO, Vector[Operation]](Vector.empty)
      tradingClient = new TestTradingClient(
        positionsR,
        balanceR,
        operationsR,
        infos.view.mapValues(_._1).toMap,
      )
      wallet <- Wallet.of[IO](registry, tradingClient, marketsAmount, thresholdPercent, spreadPercent)
    } yield WalletFixture(wallet, operationsR, balanceR, positionsR)

  private def positionsMap(positions: List[Position]): Map[String, BigDecimal] =
    positions.map(position => position.asset -> position.size).toMap

  private def positionValues(
    positions: Map[String, BigDecimal],
    infos: Map[String, (BigDecimal, BigDecimal)],
  ): Map[String, BigDecimal] =
    positions
      .map { case (asset, size) => asset -> (size * infos(asset)._1) }
      .filter { case (_, value) => value > BigDecimal("0.000001") }

  private def inRange(value: BigDecimal, from: BigDecimal, to: BigDecimal) =
    expect(clue((value, from, to))._1 >= from) and expect(clue((value, from, to))._1 <= to)

  private def testRepository(
    infos: Map[String, (BigDecimal, BigDecimal)],
    now: Instant,
  ): TokensInfoRepository[IO] =
    new TokensInfoRepository[IO] {
      override def insert(tokens: NonEmptyList[TokenInfo]): IO[Unit] =
        IO.unit

      override def select(now: Instant): IO[List[TokenInfo]] =
        infos.toList.map {
          case (id, (price, efficiency)) =>
            val secondsToResolve = BigDecimal(100000)
            val score            = efficiency * secondsToResolve / (1 - price)

            TokenInfo(
              id               = id,
              price            = price,
              score            = score,
              resolveDate      = now.plusSeconds(secondsToResolve.toLong),
              lastUpdatedBlock = 0,
            )
        }.pure[IO]
    }

  test("sells redundant and replaced positions before buying better top market") {
    val infos = Map(
      "a" -> (BigDecimal("0.5"), BigDecimal(100)),
      "b" -> (BigDecimal("0.5"), BigDecimal(10)),
      "c" -> (BigDecimal("0.5"), BigDecimal(80)),
      "d" -> (BigDecimal("0.5"), BigDecimal(200)),
    )
    val positions = List(Position("a", 10), Position("b", 10), Position("c", 10))

    for {
      fixture     <- walletWith(infos, positions, balance = 0, marketsAmount = 2)
      _           <- fixture.wallet.updateWallet()
      operations  <- fixture.operationsR.get
      finalBalance <- fixture.balanceR.get
      finalPositions <- fixture.positionsR.get.map(positions => positionValues(positionsMap(positions), infos))
    } yield expect.same(List("b", "c"), operations.collect { case Operation.Sell(tokenId, _, _) => tokenId }.take(2)) and
      expect(operations.exists {
        case Operation.Buy("d", _, _) => true
        case _                       => false
      }) and
      expect(finalBalance >= 0) and
      expect(finalBalance < BigDecimal("0.000001")) and
      expect.same(Set("a", "d"), finalPositions.keySet) and
      inRange(finalPositions("a"), BigDecimal("7.35"), BigDecimal("7.36")) and
      inRange(finalPositions("d"), BigDecimal("7.25"), BigDecimal("7.26"))
  }

  test("keeps current top market when candidate efficiency is inside threshold") {
    val infos = Map(
      "a" -> (BigDecimal("0.5"), BigDecimal(100)),
      "c" -> (BigDecimal("0.5"), BigDecimal(80)),
      "d" -> (BigDecimal("0.5"), BigDecimal(85)),
    )
    val positions = List(Position("a", 10), Position("c", 10))

    for {
      fixture     <- walletWith(infos, positions, balance = 42, marketsAmount = 2)
      _           <- fixture.wallet.updateWallet()
      operations  <- fixture.operationsR.get
      finalBalance <- fixture.balanceR.get
      finalPositions <- fixture.positionsR.get.map(positions => positionValues(positionsMap(positions), infos))
    } yield expect(operations.collect { case Operation.Sell(tokenId, _, _) => tokenId }.isEmpty) and
      expect(!operations.exists(_.tokenId == "d")) and
      expect(finalBalance >= 0) and
      expect(finalBalance < BigDecimal("0.000001")) and
      expect.same(Set("a", "c"), finalPositions.keySet) and
      inRange(finalPositions("a"), BigDecimal("25.58"), BigDecimal("25.59")) and
      inRange(finalPositions("c"), BigDecimal("25.58"), BigDecimal("25.59"))
  }

  test("sells overweight target position after redundant sells and before missing buys") {
    val infos = Map(
      "a" -> (BigDecimal("0.5"), BigDecimal(100)),
      "b" -> (BigDecimal("0.5"), BigDecimal(10)),
      "d" -> (BigDecimal("0.5"), BigDecimal(200)),
      "e" -> (BigDecimal("0.5"), BigDecimal(150)),
    )
    val positions = List(Position("a", 300), Position("b", 10))

    for {
      fixture     <- walletWith(infos, positions, balance = 0, marketsAmount = 3)
      _           <- fixture.wallet.updateWallet()
      operations  <- fixture.operationsR.get
      finalBalance <- fixture.balanceR.get
      finalPositions <- fixture.positionsR.get.map(positions => positionValues(positionsMap(positions), infos))
    } yield expect.same(List("b", "a"), operations.collect { case Operation.Sell(tokenId, _, _) => tokenId }.take(2)) and
      expect(operations.drop(2).exists {
        case Operation.Buy(tokenId, _, _) => Set("d", "e").contains(tokenId)
        case _                           => false
      }) and expect(finalBalance >= 0) and
      expect(finalBalance < BigDecimal("0.000001")) and
      expect.same(Set("a", "d"), finalPositions.keySet) and
      inRange(finalPositions("a"), BigDecimal("103.26"), BigDecimal("103.27")) and
      inRange(finalPositions("d"), BigDecimal("49.70"), BigDecimal("49.71"))
  }
}
