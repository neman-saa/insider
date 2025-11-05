import scala.collection.immutable.HashMap

val map = HashMap.empty[(String, String), (BigDecimal, BigDecimal)]

val newRes = (
  BigDecimal(5) + BigDecimal(6) * BigDecimal(10),
  BigDecimal(5) + BigDecimal(6) * BigDecimal(10)
)
map + (("gg", "ff") -> newRes)