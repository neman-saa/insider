package org.github.insider.polymarket.client

import cats.effect.{IO, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.domain.{BuyOrder, BuyOrderResult, Order, Position, SellOrder, SellOrderResult}
import weaver.SimpleIOSuite

object SimulatedTradingClientSpec extends SimpleIOSuite {

  private val Token  = "tokenA"
  private val Token2 = "tokenB"

  /** Minimal stub: only `ordersList` is exercised by `SimulatedTradingClient`. */
  private final class BookStub(books: Map[String, List[Order]]) extends TradingClient[IO] {
    override def ordersList(tokenId: String): IO[List[Order]] =
      IO.pure(books.getOrElse(tokenId, List.empty))

    override def buy(t: String, m: BigDecimal, p: Option[BigDecimal])  = IO.raiseError(new NotImplementedError)
    override def sell(t: String, s: BigDecimal, p: Option[BigDecimal]) = IO.raiseError(new NotImplementedError)
    override def balance()                                             = IO.raiseError(new NotImplementedError)
    override def positions(u: Option[String])                          = IO.raiseError(new NotImplementedError)
    override def buyOrder(t: String, a: BigDecimal, p: BigDecimal)     = IO.raiseError(new NotImplementedError)
    override def sellOrder(t: String, s: BigDecimal, p: BigDecimal)    = IO.raiseError(new NotImplementedError)
    override def portfolioValue(u: Option[String])                     = IO.raiseError(new NotImplementedError)
  }

  /** Book that flips between two snapshots so we can test mark-to-market against an updated quote. */
  private final class DynamicBookStub(snapshots: Ref[IO, Map[String, List[Order]]]) extends TradingClient[IO] {
    override def ordersList(tokenId: String): IO[List[Order]] =
      snapshots.get.map(_.getOrElse(tokenId, List.empty))

    override def buy(t: String, m: BigDecimal, p: Option[BigDecimal])  = IO.raiseError(new NotImplementedError)
    override def sell(t: String, s: BigDecimal, p: Option[BigDecimal]) = IO.raiseError(new NotImplementedError)
    override def balance()                                             = IO.raiseError(new NotImplementedError)
    override def positions(u: Option[String])                          = IO.raiseError(new NotImplementedError)
    override def buyOrder(t: String, a: BigDecimal, p: BigDecimal)     = IO.raiseError(new NotImplementedError)
    override def sellOrder(t: String, s: BigDecimal, p: BigDecimal)    = IO.raiseError(new NotImplementedError)
    override def portfolioValue(u: Option[String])                     = IO.raiseError(new NotImplementedError)
  }

  private def book(orders: Order*): Map[String, List[Order]] = Map(Token -> orders.toList)

  private def mkSim(
    books: Map[String, List[Order]],
    initialBalance: BigDecimal                = BigDecimal(100),
    initialPortfolio: Map[String, BigDecimal] = Map.empty,
  ): IO[TradingClient[IO]] =
    SimulatedTradingClient.of[IO](new BookStub(books), initialBalance, initialPortfolio)

  // ---------------------------- BUY ----------------------------

  test("buy: single ask level, full fill") {
    // 50 @ 0.40 = 20 USDC for 50 shares
    val asks = List(SellOrder(size = 50, price = BigDecimal("0.40")))
    for {
      sim <- mkSim(book(asks: _*))
      res <- sim.buy(Token, money = BigDecimal(20), maxPrice = None)
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(res == BuyOrderResult(amount = BigDecimal(50), totalPrice = BigDecimal(20))) and
      expect(bal == BigDecimal(80)) and
      expect(pos == List(Position(Token, BigDecimal(50))))
  }

  test("buy: walks multiple ask levels in ascending price order") {
    val asks = List(
      SellOrder(size = 10, price = BigDecimal("0.50")), // unsorted on purpose
      SellOrder(size = 20, price = BigDecimal("0.30")),
      SellOrder(size = 5, price  = BigDecimal("0.40")),
    )
    // budget = 100. Order after sort: 0.30 x 20 (=6), 0.40 x 5 (=2), 0.50 x 10 (=5) → total 13 USDC, 35 shares
    for {
      sim <- mkSim(book(asks: _*), initialBalance = BigDecimal(100))
      res <- sim.buy(Token, BigDecimal(100), maxPrice = None)
      bal <- sim.balance()
    } yield expect(res.amount == BigDecimal(35)) and
      expect(res.totalPrice == BigDecimal(13)) and
      expect(bal == BigDecimal(87))
  }

  test("buy: partial fill at last level when budget is exhausted mid-level") {
    val asks = List(
      SellOrder(size = 10, price = BigDecimal("0.40")), //  4 USDC for 10 shares
      SellOrder(size = 50, price = BigDecimal("0.50")), // budget left = 6 → 12 shares
    )
    for {
      sim <- mkSim(book(asks: _*))
      res <- sim.buy(Token, BigDecimal(10), maxPrice = None)
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(res.amount == BigDecimal(22)) and
      expect(res.totalPrice == BigDecimal(10)) and
      expect(bal == BigDecimal(90)) and
      expect(pos == List(Position(Token, BigDecimal(22))))
  }

  test("buy: respects maxPrice and stops walking when ask.price > maxPrice") {
    val asks = List(
      SellOrder(size = 10, price = BigDecimal("0.40")),
      SellOrder(size = 50, price = BigDecimal("0.55")), // skipped, exceeds cap
    )
    for {
      sim <- mkSim(book(asks: _*))
      res <- sim.buy(Token, BigDecimal(100), maxPrice = Some(BigDecimal("0.50")))
      bal <- sim.balance()
    } yield expect(res.amount == BigDecimal(10)) and
      expect(res.totalPrice == BigDecimal(4)) and
      expect(bal == BigDecimal(96))
  }

  test("buy: raises IllegalStateException when balance < money") {
    val asks = List(SellOrder(size = 100, price = BigDecimal("0.50")))
    for {
      sim <- mkSim(book(asks: _*), initialBalance = BigDecimal(10))
      err <- sim.buy(Token, BigDecimal(50), maxPrice = None).attempt
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(err.isLeft) and
      expect(err.left.exists(_.isInstanceOf[IllegalStateException])) and
      expect(bal == BigDecimal(10)) and // state untouched
      expect(pos.isEmpty)
  }

  test("buy: empty asks → zero fill, balance untouched") {
    for {
      sim <- mkSim(books = Map(Token -> List.empty))
      res <- sim.buy(Token, BigDecimal(10), maxPrice = None)
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(res == BuyOrderResult(BigDecimal(0), BigDecimal(0))) and
      expect(bal == BigDecimal(100)) and
      expect(pos.isEmpty)
  }

  test("buy: maxPrice below best ask → zero fill") {
    val asks = List(SellOrder(size = 10, price = BigDecimal("0.40")))
    for {
      sim <- mkSim(book(asks: _*))
      res <- sim.buy(Token, BigDecimal(10), maxPrice = Some(BigDecimal("0.30")))
      bal <- sim.balance()
    } yield expect(res.amount == BigDecimal(0)) and
      expect(res.totalPrice == BigDecimal(0)) and
      expect(bal == BigDecimal(100))
  }

  // ---------------------------- SELL ----------------------------

  test("sell: single bid level, full fill") {
    val bids = List(BuyOrder(size = 50, price = BigDecimal("0.60")))
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(50)))
      res <- sim.sell(Token, BigDecimal(50), minPrice = None)
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(res == SellOrderResult(amount = BigDecimal(50), totalPrice = BigDecimal(30))) and
      expect(bal == BigDecimal(130)) and
      expect(pos.isEmpty) // fully closed → tokenId dropped from map
  }

  test("sell: walks multiple bid levels in descending price order") {
    val bids = List(
      BuyOrder(size = 5, price  = BigDecimal("0.50")), // unsorted on purpose
      BuyOrder(size = 10, price = BigDecimal("0.70")),
      BuyOrder(size = 20, price = BigDecimal("0.60")),
    )
    // Sorted desc by price: 0.70 x 10 (=7), 0.60 x 20 (=12), 0.50 x 5 (=2.5) → 35 shares, 21.5 USDC
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(35)))
      res <- sim.sell(Token, BigDecimal(35), minPrice = None)
      bal <- sim.balance()
    } yield expect(res.amount == BigDecimal(35)) and
      expect(res.totalPrice == BigDecimal("21.5")) and
      expect(bal == BigDecimal("121.5"))
  }

  test("sell: capped by held shares, never goes short") {
    val bids = List(BuyOrder(size = 100, price = BigDecimal("0.60")))
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(10)))
      res <- sim.sell(Token, BigDecimal(50), minPrice = None) // ask to sell 50, but hold only 10
      pos <- sim.positions()
    } yield expect(res.amount == BigDecimal(10)) and
      expect(res.totalPrice == BigDecimal(6)) and
      expect(pos.isEmpty)
  }

  test("sell: respects minPrice and stops when bid.price < minPrice") {
    val bids = List(
      BuyOrder(size = 5, price  = BigDecimal("0.70")),
      BuyOrder(size = 30, price = BigDecimal("0.40")), // skipped, below floor
    )
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(50)))
      res <- sim.sell(Token, BigDecimal(50), minPrice = Some(BigDecimal("0.60")))
      pos <- sim.positions()
    } yield expect(res.amount == BigDecimal(5)) and
      expect(res.totalPrice == BigDecimal("3.5")) and
      expect(pos == List(Position(Token, BigDecimal(45)))) // 50 - 5 = 45 still held
  }

  test("sell: nothing held → zero fill, balance untouched, no book read needed") {
    val bids = List(BuyOrder(size = 100, price = BigDecimal("0.99")))
    for {
      sim <- mkSim(book(bids: _*))
      res <- sim.sell(Token, BigDecimal(10), minPrice = None)
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(res == SellOrderResult(BigDecimal(0), BigDecimal(0))) and
      expect(bal == BigDecimal(100)) and
      expect(pos.isEmpty)
  }

  test("sell: partial fill at last bid level when shares < bid.size") {
    val bids = List(BuyOrder(size = 100, price = BigDecimal("0.60")))
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(15)))
      res <- sim.sell(Token, BigDecimal(15), minPrice = None)
      pos <- sim.positions()
    } yield expect(res.amount == BigDecimal(15)) and
      expect(res.totalPrice == BigDecimal(9)) and
      expect(pos.isEmpty)
  }

  // ---------------------------- LIMIT ORDERS ----------------------------

  test("buyOrder: IOC fill against book; unfilled budget is dropped") {
    // Limit buy 50 shares @ 0.40. Book offers only 10 @ 0.40, rest @ 0.55.
    val asks = List(
      SellOrder(size = 10, price  = BigDecimal("0.40")),
      SellOrder(size = 100, price = BigDecimal("0.55")),
    )
    for {
      sim <- mkSim(book(asks: _*))
      _   <- sim.buyOrder(Token, amount = BigDecimal(50), price = BigDecimal("0.40"))
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(bal == BigDecimal(96)) and // spent 4 USDC on 10 shares
      expect(pos == List(Position(Token, BigDecimal(10))))
  }

  test("sellOrder: IOC fill against book; unfilled remainder is dropped") {
    val bids = List(
      BuyOrder(size = 10, price  = BigDecimal("0.70")),
      BuyOrder(size = 100, price = BigDecimal("0.40")),
    )
    for {
      sim <- mkSim(book(bids: _*), initialPortfolio = Map(Token -> BigDecimal(50)))
      _   <- sim.sellOrder(Token, shares = BigDecimal(50), price = BigDecimal("0.60"))
      bal <- sim.balance()
      pos <- sim.positions()
    } yield expect(bal == BigDecimal(107)) and // got 7 USDC for 10 shares at 0.70
      expect(pos == List(Position(Token, BigDecimal(40)))) // 50 - 10 = 40 still held
  }

  // ---------------------------- PORTFOLIO VALUE ----------------------------

  test("portfolioValue: marks every position at best bid and adds cash") {
    val books = Map(
      Token  -> List(BuyOrder(size = 100, price = BigDecimal("0.60")), BuyOrder(size = 5, price = BigDecimal("0.55"))),
      Token2 -> List(BuyOrder(size = 50, price = BigDecimal("0.20"))),
    )
    for {
      sim <- mkSim(books, initialBalance = BigDecimal(50), initialPortfolio = Map(Token -> 10, Token2 -> 20))
      v   <- sim.portfolioValue()
    } yield // cash 50 + Token: 10 * 0.60 = 6 + Token2: 20 * 0.20 = 4 → 60
      expect(v == BigDecimal(60))
  }

  test("portfolioValue: empty bid side ⇒ position valued at zero") {
    val books = Map(Token -> List(SellOrder(size = 100, price = BigDecimal("0.40")))) // only asks, no bids
    for {
      sim <- mkSim(books, initialBalance = BigDecimal(25), initialPortfolio = Map(Token -> BigDecimal(10)))
      v   <- sim.portfolioValue()
    } yield expect(v == BigDecimal(25)) // 25 cash + 0
  }

  // ---------------------------- ROUND-TRIP / STATE ----------------------------

  test("round-trip: buy then sell adjusts balance by the bid/ask spread") {
    // Buy 20 shares @ 0.40 = 8 USDC; then sell 20 shares @ 0.30 = 6 USDC. Net loss: 2 USDC.
    val asks = List(SellOrder(size = 20, price = BigDecimal("0.40")))
    val bids = List(BuyOrder(size = 20, price = BigDecimal("0.30")))
    for {
      snapshots <- Ref.of[IO, Map[String, List[Order]]](Map(Token -> asks))
      stub       = new DynamicBookStub(snapshots)
      sim       <- SimulatedTradingClient.of[IO](stub, initialBalance = BigDecimal(100))
      _         <- sim.buy(Token, BigDecimal(8), maxPrice = None)
      _         <- snapshots.set(Map(Token -> bids))
      _         <- sim.sell(Token, BigDecimal(20), minPrice = None)
      bal       <- sim.balance()
      pos       <- sim.positions()
    } yield expect(bal == BigDecimal(98)) and expect(pos.isEmpty)
  }

  test("ordersList: delegates to underlying real client unchanged") {
    val asks = List(SellOrder(size = 1, price = BigDecimal("0.50")))
    val bids = List(BuyOrder(size = 2, price = BigDecimal("0.45")))
    for {
      sim <- mkSim(Map(Token -> (asks ++ bids)))
      ob  <- sim.ordersList(Token)
    } yield expect(ob.size == 2) and
      expect(ob.collect { case s: SellOrder => s } == asks) and
      expect(ob.collect { case b: BuyOrder => b } == bids)
  }
}
