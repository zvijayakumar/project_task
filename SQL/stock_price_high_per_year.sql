with high_cte as ( 
select ticker,highest_mth,highest_open
from ( 
SELECT ticker,TO_CHAR(date, 'Mon-YYYY') as highest_mth,high as highest_open,rank() over(partition by ticker,extract(year from date) order by high desc) as rnk
FROM stock_prices 
) a where a.rnk = 1
) ,
low_cte as ( 

select ticker,lowest_mth,lowest_open
from ( 
SELECT ticker,TO_CHAR(date, 'Mon-YYYY') as lowest_mth,low as lowest_open,rank() over(partition by ticker,extract(year from date) order by low) as rnk
FROM stock_prices 
) a where a.rnk = 1

) 
select h.ticker,h.highest_mth,h.highest_open,l.lowest_mth,l.lowest_open
from high_cte h join low_cte l on h.ticker = l.ticker 
and split_part(l.lowest_mth,'-',2) = split_part(h.highest_mth,'-',2) 
order by h.ticker

--


with high_cte as ( 
select ticker,highest_mth,highest_open
from ( 
SELECT ticker,TO_CHAR(date, 'Mon-YYYY') as highest_mth,open as highest_open,row_number() over(partition by ticker order by open desc) as rnk
FROM stock_prices  
) a where a.rnk = 1
) ,
low_cte as ( 

select ticker,lowest_mth,lowest_open
from ( 
SELECT ticker,TO_CHAR(date, 'Mon-YYYY') as lowest_mth,open as lowest_open,row_number() over(partition by ticker order by open) as rnk
FROM stock_prices  
) a where a.rnk = 1

) 
select h.ticker,h.highest_mth,h.highest_open,l.lowest_mth,l.lowest_open
from high_cte h join low_cte l on h.ticker = l.ticker 
order by h.ticker
