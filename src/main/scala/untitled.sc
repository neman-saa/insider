import org.http4s.Uri

Uri.unsafeFromString("https://gamma-api.polymarket.com")
  .withQueryParams(Map("fff" -> "fff", "fff" -> "fff")).toString()

"""User address: 0x59d4a69c0da718e44ff33f8690c314481157b745
  |Operation side: Sell
  |Tokens amount: 35.714284
  |Total price: 4.999999
  |Single token price: 0.14
  |Block Timestamp: 2026-03-16T22:49:39
  |Token ID: 22004963719896069538353887145526141418500847861039690949252867096400053103166
  |Event link: https://polymarket.com/event/highest-temperature-in-seoul-on-march-17-2026
  |Event slug: highest-temperature-in-seoul-on-march-17-2026
  |Market question: Will the highest temperature in Seoul be 10°C on March 17?
  |
  |Leaderboard name: Win Rate Leaderboard
  |Leaderboard stat: rank - 613/10000, win - 432, lose - 110""".stripMargin.length