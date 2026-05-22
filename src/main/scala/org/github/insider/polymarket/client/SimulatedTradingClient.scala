package org.github.insider.polymarket.client

import cats.effect.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._
import org.github.insider.polymarket.domain.{BuyOrder, BuyOrderResult, Order, Position, SellOrder, SellOrderResult}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
  * In-memory simulation of [[TradingClient]] for backtesting / dry-runs.
  *
  * Pricing is driven by the real order book — `ordersList` is delegated to the underlying [[TradingClient]]. Balance
  * (USDC) and portfolio (tokenId -> shares) are kept locally in `Ref`s and atomically updated on every fill.
  *
  * Behavior:
  *   - `buy(money, maxPrice)` walks asks ascending by price, consuming liquidity until the budget is spent or
  *     `maxPrice` is exceeded. If `balance < money` the call fails fast with `IllegalStateException` before touching
  *     the book.
  *   - `sell(shares, minPrice)` walks bids descending by price, consuming liquidity until the requested size is filled
  *     or `minPrice` is violated. Cannot sell more than what is currently held in the portfolio.
  *   - `buyOrder` / `sellOrder` (limit) are simulated as IOC-style fills against the book at or better than the limit
  *     price. Any unfilled remainder is logged and dropped — resting orders are NOT persisted.
  *   - `portfolioValue` marks every position at the current best bid (what we could realistically sell for right now).
  *     Empty bid side ⇒ 0.
  *
  * Concurrency: a fair `Semaphore(1)` serializes every mutating operation so that book-read + state-update is atomic.
  */
private class SimulatedTradingClient[F[_]: Async](
  realClient: TradingClient[F],
  balanceRef: Ref[F, BigDecimal],
  portfolioRef: Ref[F, Map[String, BigDecimal]],
  lock: Semaphore[F],
  logger: Logger[F],
) extends TradingClient[F] {

  import SimulatedTradingClient.Fill

  override def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[BuyOrderResult] =
    lock.permit.use { _ =>
      for {
        bal <- balanceRef.get
        _ <-
          if (bal < money)
            logger.error(s"[SIM] BUY $tokenId rejected: insufficient balance=$bal, requested=$money") *>
              Async[F].raiseError[Unit](
                new IllegalStateException(s"Insufficient balance: have $bal, need $money for token $tokenId")
              )
          else Async[F].unit
        book <- realClient.ordersList(tokenId)
        asks  = book.collect { case s: SellOrder => s }.sortBy(_.price)
        fill  = walkAsks(asks, money, maxPrice)
        _ <-
          if (fill.size <= BigDecimal(0))
            logger.warn(
              s"[SIM] BUY $tokenId no fill: money=$money maxPrice=$maxPrice balance=$bal asks=${asks.size}"
            )
          else
            balanceRef.update(_ - fill.cash) *>
              portfolioRef.update(p => p.updated(tokenId, p.getOrElse(tokenId, BigDecimal(0)) + fill.size)) *>
              logger.info(
                s"[SIM] BUY $tokenId: shares=${fill.size} spent=${fill.cash} " +
                  s"avgPrice=${if (fill.size > 0) fill.cash / fill.size else BigDecimal(0)} " +
                  s"maxPrice=$maxPrice"
              )
      } yield BuyOrderResult(amount = fill.size, totalPrice = fill.cash)
    }

  override def sell(tokenId: String, shares: BigDecimal, minPrice: Option[BigDecimal]): F[SellOrderResult] =
    lock.permit.use { _ =>
      for {
        portfolio <- portfolioRef.get
        held       = portfolio.getOrElse(tokenId, BigDecimal(0))
        toSell     = held.min(shares)
        result <-
          if (toSell <= 0)
            logger.warn(s"[SIM] SELL $tokenId rejected: held=$held requested=$shares") *>
              SellOrderResult(0, 0).pure[F]
          else
            for {
              book <- realClient.ordersList(tokenId)
              bids  = book.collect { case b: BuyOrder => b }.sortBy(o => -o.price)
              fill  = walkBids(bids, toSell, minPrice)
              _ <-
                if (fill.size <= BigDecimal(0))
                  logger.warn(
                    s"[SIM] SELL $tokenId no liquidity: requested=$toSell minPrice=$minPrice bids=${bids.size}"
                  )
                else
                  balanceRef.update(_ + fill.cash) *>
                    portfolioRef.update { p =>
                      val left = p.getOrElse(tokenId, BigDecimal(0)) - fill.size
                      if (left <= 0) p - tokenId else p.updated(tokenId, left)
                    } *>
                    logger.info(
                      s"[SIM] SELL $tokenId: shares=${fill.size} received=${fill.cash} " +
                        s"avgPrice=${if (fill.size > 0) fill.cash / fill.size else BigDecimal(0)} " +
                        s"minPrice=$minPrice"
                    )
            } yield SellOrderResult(amount = fill.size, totalPrice = fill.cash)
      } yield result
    }

  override def balance(): F[BigDecimal] = balanceRef.get

  override def positions(user: Option[String] = None): F[List[Position]] =
    portfolioRef
      .get
      .map(_.iterator.collect {
        case (tokenId, size) if size > 0 => Position(asset = tokenId, size = size)
      }.toList)

  /** Limit buy: matches whatever is in the book at price ≤ `price`. Remainder is dropped. */
  override def buyOrder(tokenId: String, amount: BigDecimal, price: BigDecimal): F[Unit] = {
    val budget = amount * price
    buy(tokenId, budget, Some(price)).flatMap { res =>
      val unfilledBudget = budget - res.totalPrice
      if (unfilledBudget > 0)
        logger.info(
          s"[SIM] buyOrder $tokenId @ $price partially filled: shares=${res.amount} " +
            s"spent=${res.totalPrice} unfilledBudget=$unfilledBudget (not kept as resting)"
        )
      else Async[F].unit
    }
  }

  /** Limit sell: matches whatever is in the book at price ≥ `price`. Remainder is dropped. */
  override def sellOrder(tokenId: String, shares: BigDecimal, price: BigDecimal): F[Unit] =
    sell(tokenId, shares, Some(price)).flatMap { res =>
      val unfilled = shares - res.amount
      if (unfilled > 0)
        logger.info(
          s"[SIM] sellOrder $tokenId @ $price partially filled: sold=${res.amount} " +
            s"received=${res.totalPrice} unfilled=$unfilled shares (not kept as resting)"
        )
      else Async[F].unit
    }

  /** Mark-to-market at current best bid. `user` argument is ignored (the sim has a single account). */
  override def portfolioValue(user: Option[String] = None): F[BigDecimal] =
    for {
      portfolio <- portfolioRef.get
      shares <- portfolio.toList.traverse {
        case (tokenId, size) =>
          realClient.ordersList(tokenId).map { book =>
            val bestBid =
              book.collect { case b: BuyOrder => b.price }.maxOption.getOrElse(BigDecimal(0))
            size * bestBid
          }
      }
      cash <- balanceRef.get
    } yield cash + shares.sum

  override def ordersList(tokenId: String): F[List[Order]] = realClient.ordersList(tokenId)

  // ---------- pure matching engine ----------

  /** Walk asks ascending by price; spend up to `budget`, stop when ask.price > maxPrice. */
  private def walkAsks(
    asks: List[SellOrder],
    budget: BigDecimal,
    maxPrice: Option[BigDecimal],
  ): Fill = {
    val zero = BigDecimal(0)
    val (size, cash, _) = asks.foldLeft((zero, zero, budget)) {
      case ((accSize, accCash, remaining), ask) =>
        if (remaining <= 0) (accSize, accCash, remaining)
        else if (maxPrice.exists(ask.price > _)) (accSize, accCash, remaining)
        else {
          val levelCost = ask.size * ask.price
          if (remaining >= levelCost)
            (accSize + ask.size, accCash + levelCost, remaining - levelCost)
          else {
            // partial fill at this level
            val take = remaining / ask.price
            (accSize + take, accCash + remaining, zero)
          }
        }
    }
    Fill(size, cash)
  }

  /** Walk bids descending by price; sell up to `shares`, stop when bid.price < minPrice. */
  private def walkBids(
    bids: List[BuyOrder],
    shares: BigDecimal,
    minPrice: Option[BigDecimal],
  ): Fill = {
    val zero = BigDecimal(0)
    val (size, cash, _) = bids.foldLeft((zero, zero, shares)) {
      case ((accSize, accCash, remaining), bid) =>
        if (remaining <= 0) (accSize, accCash, remaining)
        else if (minPrice.exists(bid.price < _)) (accSize, accCash, remaining)
        else if (remaining >= bid.size)
          (accSize + bid.size, accCash + bid.size * bid.price, remaining - bid.size)
        else
          (accSize + remaining, accCash + remaining * bid.price, zero)
    }
    Fill(size, cash)
  }
}

object SimulatedTradingClient {

  /** Filled shares + cash leg (spent on buy, received on sell). */
  private final case class Fill(size: BigDecimal, cash: BigDecimal)

  def of[F[_]: Async](
    realClient: TradingClient[F],
    initialBalance: BigDecimal,
    initialPortfolio: Map[String, BigDecimal] = Map.empty,
  ): F[TradingClient[F]] =
    for {
      balanceRef   <- Ref.of[F, BigDecimal](initialBalance)
      portfolioRef <- Ref.of[F, Map[String, BigDecimal]](initialPortfolio)
      lock         <- Semaphore[F](1)
      logger       <- Slf4jLogger.create[F]
      _ <- logger.info(s"[SIM] SimulatedTradingClient started. balance=$initialBalance portfolio=$initialPortfolio")
    } yield new SimulatedTradingClient[F](realClient, balanceRef, portfolioRef, lock, logger)
}
