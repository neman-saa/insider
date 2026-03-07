package org.github.insider.alchemy.domain

case class User(address: String, newMoney: BigDecimal, balance: BigDecimal, balanceFromTokens: BigDecimal)
