package org.github.insider.simulations

import cats.data.NonEmptyList

import java.time.Instant

final case class WalletsPool(
  primary: Wallet,
  temporary: List[Wallet],
) { self =>

  def resolveTokens(
    tokenResolutions: Map[TokenId, TokenResolutionInfo],
    maybeLastestBlockTimestamp: Option[Instant],
    lockProfit: Boolean,
  ): WalletsPool = {
    maybeLastestBlockTimestamp match {
      case Some(latestBlockTimestamp) =>
        val walletsNel: NonEmptyList[Wallet] = self.toNel
        val resolvedWallets: NonEmptyList[Wallet] =
          walletsNel.map(wallet => wallet.resolveTokens(tokenResolutions, Some(latestBlockTimestamp), lockProfit))

        WalletsPool.fromNel(resolvedWallets)
      case None =>
        self
    }
  }

  def removeExpiredWallets(
    tokenResolutions: Map[TokenId, TokenResolutionInfo],
    currentBlock: Int,
    sellActiveTokensAtResolutionPrice: Boolean,
  ): (WalletsPool, List[Wallet]) = {
    val (remainedWallets, expiredWallets): (List[Wallet], List[Wallet]) =
      self.temporary.partition(wallet => wallet.activeToBlock.exists(activeTo => activeTo > currentBlock))

    val resolvedExpiredWallets: List[Wallet] =
      if (sellActiveTokensAtResolutionPrice)
        expiredWallets.map(wallet => wallet.resolveTokens(tokenResolutions, resolvedBefore = None, lockProfit = false))
      else expiredWallets

    (self.copy(temporary = remainedWallets), resolvedExpiredWallets)
  }

  def addWallets(wallets: List[Wallet]): WalletsPool =
    self.copy(temporary = self.temporary.appendedAll(wallets))

  def toNel: NonEmptyList[Wallet] =
    NonEmptyList.of[Wallet](self.primary, self.temporary: _*)
}

object WalletsPool {
  def initWithPrimary(wallet: Wallet): WalletsPool =
    WalletsPool(primary = wallet, temporary = Nil)

  def fromNel(nel: NonEmptyList[Wallet]): WalletsPool =
    WalletsPool(primary = nel.head, temporary = nel.tail)
}
