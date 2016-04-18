
DROP TABLE tradingHistory;

CREATE TABLE tradingHistory (
  uid BIGINT PRIMARY KEY IDENTITY(1,1),
  symbol VARCHAR(12) NOT NULL,
  tradeDate DATETIME,
  daysOpen DECIMAL(12,5),
  daysClose DECIMAL(12,5),
  daysHigh DECIMAL(12,5),
  daysLow DECIMAL(12,5),
  volume BIGINT
);

CREATE UNIQUE INDEX tradingHistory_symbol_xpk ON tradingHistory ( symbol, tradeDate );