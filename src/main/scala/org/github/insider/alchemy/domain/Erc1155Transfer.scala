package org.github.insider.alchemy.domain


case class Erc1155Transfer(from: String, to: String, value: BigDecimal, asset: String, hash: String)
