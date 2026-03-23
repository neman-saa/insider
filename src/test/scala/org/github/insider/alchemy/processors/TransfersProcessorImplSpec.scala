package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer}
import org.github.insider.alchemy.domain.dto.TokenCategory.ERC1155
import org.github.insider.polymarket.domain.Side
import org.github.insider.polymarket.domain.Trade
import weaver.SimpleIOSuite

object TransfersProcessorImplSpec extends SimpleIOSuite {

  private val CTFAddress = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

  private val processor = TransfersProcessorImpl()

  pureTest("extract trades from 4 transfers between 2 wallets with 1 asset") {

    val usdcTransfers =
      List(
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
      )

    val erc1155Transfers =
      List(
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
      )

    val trades = processor.extractTradesFrom(usdcTransfers ++ erc1155Transfers)

    expect(trades.size == 2) and
      expect(
        trades contains Trade(
          makerAddress   = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          tokenId        = "1",
          side           = Side.Buy,
          amount         = BigDecimal(10_000_000),
          totalPrice     = BigDecimal(10),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          tokenId        = "1",
          side           = Side.Sell,
          amount         = BigDecimal(10_000_000),
          totalPrice     = BigDecimal(10),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      )
  }

  pureTest("extract trades from 8 transfers between 2 wallets with 1 asset") {

    val usdcTransfers =
      List(
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
      )

    val erc1155Transfers =
      List(
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x4C4B40", // 5_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x4C4B40", // 5_000_000
        ),
      )

    val trades = processor.extractTradesFrom(usdcTransfers ++ erc1155Transfers)

    expect(trades.size == 2) and
      expect(
        trades contains Trade(
          makerAddress   = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          tokenId        = "1",
          side           = Side.Buy,
          amount         = BigDecimal(15_000_000),
          totalPrice     = BigDecimal(15),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          tokenId        = "1",
          side           = Side.Sell,
          amount         = BigDecimal(15_000_000),
          totalPrice     = BigDecimal(15),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      )
  }

  pureTest("extract trades from 8 transfers between 2 wallets with 2 assets") {

    val usdcTransfers =
      List(
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
      )

    val erc1155Transfers =
      List(
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x2",
          value          = "0x4C4B40", // 5_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x2",
          value          = "0x4C4B40", // 5_000_000
        ),
      )

    val trades = processor.extractTradesFrom(usdcTransfers ++ erc1155Transfers)

    expect(trades.size == 4) and
      expect(
        trades contains Trade(
          makerAddress   = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          tokenId        = "1",
          side           = Side.Buy,
          amount         = BigDecimal(10_000_000),
          totalPrice     = BigDecimal(10),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          tokenId        = "2",
          side           = Side.Buy,
          amount         = BigDecimal(5_000_000),
          totalPrice     = BigDecimal(5),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          tokenId        = "1",
          side           = Side.Sell,
          amount         = BigDecimal(10_000_000),
          totalPrice     = BigDecimal(10),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          tokenId        = "2",
          side           = Side.Sell,
          amount         = BigDecimal(5_000_000),
          totalPrice     = BigDecimal(5),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      )
  }

  pureTest("extract trades from 8 transfers between 3 wallets with 1 asset") {

    val usdcTransfers =
      List(
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(10),
        ),
        USDCTransfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xb744c931896e448633e39839eb8bbd036b4642f3",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          value          = BigDecimal(5),
        ),
      )

    val erc1155Transfers =
      List(
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = "0xb744c931896e448633e39839eb8bbd036b4642f3",
          to             = CTFAddress,
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x4C4B40", // 5_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x989680", // 10_000_000
        ),
        ERC1155Transfer(
          blockNum       = 100,
          from           = CTFAddress,
          to             = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          hash           = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          blockTimestamp = None,
          tokenId        = "0x1",
          value          = "0x4C4B40", // 5_000_000
        ),
      )

    val trades = processor.extractTradesFrom(usdcTransfers ++ erc1155Transfers)

    expect(trades.size == 3) and
      expect(
        trades contains Trade(
          makerAddress   = "0xe916d7a3a33f76abcfc805a6c4c80fe1ddf44563",
          tokenId        = "1",
          side           = Side.Buy,
          amount         = BigDecimal(15_000_000),
          totalPrice     = BigDecimal(15),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f2",
          tokenId        = "1",
          side           = Side.Sell,
          amount         = BigDecimal(10_000_000),
          totalPrice     = BigDecimal(10),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      ) and expect(
        trades contains Trade(
          makerAddress   = "0xb744c931896e448633e39839eb8bbd036b4642f3",
          tokenId        = "1",
          side           = Side.Sell,
          amount         = BigDecimal(5_000_000),
          totalPrice     = BigDecimal(5),
          blockNum       = 100,
          txHash         = "0x1204306a5166535e7355eede67c530c717446d3a981cbb24a709450148574128",
          txIndex        = 0,
          blockTimestamp = None,
        )
      )
  }

}
