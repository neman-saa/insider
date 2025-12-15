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

# Polymarket Architecture & Conditional Tokens — Overview

## 1. Ethereum, Polygon, PoW и PoS

### 1.1 Ethereum

**Ethereum** — это L1-блокчейн с виртуальной машиной **EVM**, на которой выполняются смарт-контракты.

- До 2022 работал на **Proof of Work (PoW)**
- В сентябре 2022 произошёл **The Merge** → переход на **Proof of Stake (PoS)**
- Теперь блоки создают **валидаторы**, стейкающие ETH
- Майнинга больше нет

В Ethereum используется **gas** — показатель вычислительной нагрузки:
- выше нагрузка → выше gas → дороже транзакции

---

### 1.2 Polygon PoS

**Polygon PoS** — EVM-совместимая сеть (sidechain / L2-решение).

Особенности:
- дешёвые и быстрые транзакции
- собственный gas (MATIC)
- полностью совместим с Ethereum-контрактами

📌 **Polymarket работает именно в сети Polygon PoS**, а не в L1 Ethereum.

---

### 1.3 Бриджи

Для переноса USDC из Ethereum в Polygon используются **бриджи**:

1. USDC находится в сети Ethereum
2. Через Polygon Bridge или CEX выполняется перенос
3. В сети Polygon появляется USDC (wrapped или native)
4. Токены поступают на тот же адрес пользователя в Polygon

---

## 2. Смарт-контракты и стандарты токенов

### 2.1 Solidity и EVM

- Solidity → компилируется в **EVM bytecode**
- Контракт имеет:
    - адрес
    - storage
    - public / external функции
    - **events**

 **Events** — это логи транзакции внутри блока, именно их читают Alchemy и сканеры.

---

### 2.2 Стандарты ERC

#### ERC-20
Fungible-токены (монеты):

- `balanceOf`
- `transfer`, `transferFrom`
- `approve`, `allowance`

 **USDC на Polygon — ERC-20**

---

#### ERC-721
NFT:
- 1 токен = 1 уникальный объект

---

#### ERC-1155
Мульти-токен стандарт:

- один контракт → много `tokenId`
- баланс хранится как `(address, tokenId)`
- события:
    - `TransferSingle`
    - `TransferBatch`

 **Polymarket использует ERC-1155 для позиций YES / NO (positionId)**

---

## 3. Conditional Tokens Framework (CTF)

Polymarket построен поверх **Gnosis Conditional Tokens Framework**.

Основные сущности:

- **conditionId** — описание события рынка
- **collateral token** — ERC-20, обеспечивающий позиции (USDC)

### Основные функции CTF:

- `prepareCondition()` — подготовка условия
- `splitPosition()`  
  → collateral сжигается  
  → минтятся outcome-токены (YES / NO)
- `mergePositions()`  
  → сжигается полный набор outcome-токенов  
  → возвращается collateral
- `redeemPositions()`  
  → после oracle-resolution  
  → сжигаются выигрышные токены  
  → возвращается collateral

 1 USDC ↔ 1 YES + 1 NO  
Пара YES+NO = **full set**

---

## 4. Архитектура Polymarket

### 4.1 Основные компоненты

#### CTF ERC-1155 контракт
- хранит все позиции
- реализует split / merge / redeem
- **имеет разрешение списывать USDC у Exchange**

---

#### Collateral Token (USDC)
- ERC-20 в сети Polygon
- основная валюта рынков

---

#### CTF Exchange (Polymarket Exchange)
- on-chain settlement
- swap между USDC и позициями
- поддержка mint / merge внутри обмена

---

#### CLOB (Central Limit Order Book)
- off-chain ордербук
- хранит и матчингует ордера
- on-chain выполняется только settlement

---

#### Oracle / Resolution
- сообщает результат рынка
- передаёт payout-вектор в CTF

---

#### Proxy Wallet / Safe
- смарт-кошелёк пользователя
- хранит USDC и ERC-1155 позиции

---

## 5. Split / Merge / Redeem / Withdraw

### 5.1 Collateral Token
- USDC используется как залог
- split превращает USDC в outcome-токены

---

### 5.2 Split (mint)
`splipPosition()`:
- списывает collateral
- минтит YES / NO токены

---

### 5.3 Merge
`mergePositions()`:
- сжигает полный набор outcome-токенов
- возвращает collateral
- возможно **до resolution**

---

### 5.4 Redeem
После oracle-resolution:
- сжигаются **только выигрышные токены**
- возвращается соответствующая доля collateral

---

### 5.5 Withdraw
UX-операция:
- redeem (если нужно)
- ERC-20 transfer с proxy-кошелька на EOA пользователя

---

## 6. Proxy Wallet

**Proxy wallet** — смарт-контракт-кошелёк пользователя:

- создаётся при первом входе
- управляется EOA или Magic-аккаунтом
- хранит:
    - USDC
    - ERC-1155 позиции

Позволяет:
- атомарные multi-call операции
- gasless-трейдинг через relayers

---

## 7. Полный цикл пользователя

1. Вход на polymarket.com
2. Создание proxy-кошелька
3. Перевод USDC
4. Approve + setApprovalForAll
5. Создание ордера
6. Off-chain matching
7. On-chain settlement
8. Redeem после resolution
9. Withdraw USDC

---

## 8. Почему ERC-1155 + Exchange разделены

- **CTF** → математика и логика рынка
- **Exchange** → торговля и ордера

Преимущества:
- проще аудит
- повторное использование CTF
- гибкость архитектуры

