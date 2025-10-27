## Polymarket explained

The number of **YES** and **NO** tokens in a market is **always the same**.

There are two types of participants:

### 1. Minter (Market Maker)

Minters are those who create tokens and thereby increase market liquidity.  
Market liquidity refers to the presence of buy and sell orders in the order book.

How do minters do this?  
At any given moment, `price(YES) + price(NO)` is always equal to *\$1*.  
When a minter enters the market and deposits **$100**, they receive **100 YES** and **100 NO** tokens.  
They then place these tokens for sale in the order book for traders to buy.

Minters make money by setting a total combined price **greater than \$1** — for example, selling YES tokens at **\$0.55
** and NO tokens at **\$0.50**.

---

### 2. Trader

Traders are participants who want to buy **only one outcome** — either YES or NO — rather than both at once.  
They can do this **only from the order book**.  
If there are no offers (or if the available prices don’t satisfy them), they cannot enter the market.

---

### Price Formation

The price is determined **solely by the order book**.  
You can only buy tokens that are currently offered there, so the price does not change dynamically by itself.  
Once you purchase all the available tokens at a given price `x₀`, the next available buy price becomes `x₁ = x₀ + d`,  
where `d` is the difference between the current order and the next one in the order book.

An interesting fact:  
The price displayed on Polymarket in the “Buy” section (not in the order book) is the **midpoint of the bid-ask spread
**.

The **bid-ask spread** is the difference between the best available buy price (**bid**) and the best available sell
price (**ask**):

`displayed_price = (ask + bid) / 2`

However, if the spread is **greater than $0.10**, then the displayed price is the **last traded price** instead.

---

### Example

| Bid   | Ask   | Spread | Displayed Price                    |
|-------|-------|--------|------------------------------------|
| $0.47 | $0.53 | $0.06  | Midpoint = $0.50                   |
| $0.40 | $0.55 | $0.15  | Spread > $0.10 → Last traded price |

So in practice, you **cannot buy tokens at the displayed price** —  
the **real price** is the one shown in the **order book**.


