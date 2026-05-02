ALTER TABLE tokens_info ADD buy_time Nullable(DateTime(64)) DEFAULT null;
ALTER TABLE tokens_info ADD buy_price Nullable(Decimal(38, 9)) DEFAULT null;
