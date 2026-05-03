ALTER TABLE tokens_info ADD column buy_time Nullable(DateTime64(6)) DEFAULT null;
ALTER TABLE tokens_info ADD column buy_price Nullable(Decimal(38, 9)) DEFAULT null;