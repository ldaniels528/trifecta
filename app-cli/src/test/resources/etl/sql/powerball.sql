CREATE TABLE powerball_history (
  uid BIGINT PRIMARY KEY IDENTITY (1, 1),
  draw_num VARCHAR (20) NOT NULL,
  draw_date DATETIME  NOT NULL,
  number_1 VARCHAR(3) NOT NULL,
  number_2 VARCHAR(3) NOT NULL,
  number_3 VARCHAR(3) NOT NULL,
  number_4 VARCHAR(3) NOT NULL,
  number_5 VARCHAR(3) NOT NULL,
  powerball VARCHAR(3) NOT NULL
)