---------Query 01: Calc Quantity of items, Sales value & Order quantity by each Subcategory in last 12M
		-- Tính số lượng item, giá trị doanh số và lượng đơn hàng theo mỗi danh mục con trong 12 tháng gần nhất

SELECT 
  FORMAT_DATE('%b %Y',t2.ModifiedDate) AS period,
  t1.Subcategory AS Name,
  SUM(t2.OrderQty) AS qty_item,
  SUM(t2.LineTotal) AS total_sales,
  COUNT(DISTINCT t2.SalesOrderID) AS order_cnt
FROM `adventureworks2019.Sales.Product`  AS t1
LEFT JOIN `adventureworks2019.Sales.SalesOrderDetail` AS t2
  ON t1.ProductID = t2.ProductID
WHERE date(t2.ModifiedDate) >= (SELECT date_sub(max(date(t2.ModifiedDate)), INTERVAL 12 month)
				FROM `adventureworks2019.Sales.SalesOrderDetail`)
GROUP BY Name, period
ORDER BY period DESC, Name;


---------Query 02: Calc % YoY growth rate by SubCategory & release top 3 cat WITH highest grow rate. Round results to 2 decimal
		-- Tính tỷ lệ tăng trưởng % YoY theo Danh mục phụ và hiển thị top 3 danh mục có tốc độ tăng trưởng cao nhất.
		-- Làm tròn kết quả đến 2 số thập phân.
WITH 
sale_info AS (
  SELECT 
      FORMAT_TIMESTAMP("%Y", a.ModifiedDate) AS yr,
      c.Name,
      SUM(a.OrderQty) AS qty_item
  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Production.Product` b 
  	ON a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c 
  	ON cast(b.ProductSubcategoryID AS int) = c.ProductSubcategoryID
  GROUP BY 1,2
  ORDER BY 2 ASC , 1 DESC
),

sale_diff AS (
  SELECT *,
  LEAD (qty_item) OVER (PARTITION BY Name ORDER BY yr DESC) AS prv_qty,
  ROUND(qty_item / (LEAD (qty_item) OVER (PARTITION BY Name ORDER BY yr DESC)) -1
  	,2)
  AS qty_diff
  FROM sale_info
  ORDER BY 5 DESC 
)

SELECT DISTINCT Name,
      qty_item,
      prv_qty,
      qty_diff
FROM sale_diff 
WHERE qty_diff > 0
ORDER BY qty_diff DESC 
LIMIT 3;


---------Query 03: Ranking Top 3 TeritoryID with biggest Order quantity of every year.
		-- If there's TerritoryID with same quantity in a year, do not skip the rank number
		-- Xếp hạng Top 3 TeritoryID có số lượng đặt hàng lớn nhất mỗi năm. Không bỏ qua STT đồng hạng

SELECT *
FROM(
  SELECT
    *,
    DENSE_RANK() OVER(PARTITION BY year ORDER BY order_cnt DESC) AS rk
  FROM(
    SELECT 
      EXTRACT(year FROM date(t1.ModifiedDate)) AS year,
      TerritoryID,
      SUM(t1.OrderQty) AS order_cnt
    FROM `adventureworks2019.Sales.SalesOrderDetail` AS t1
    LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` AS t2
    USING(SalesOrderID)
    GROUP BY year, TerritoryID
    )
  ORDER BY year DESC
)
WHERE rk <= 3;


---------Query 04: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
		-- Tính tổng chi phí chiết khấu thuộc về Giảm giá theo mùa cho từng Danh mục phụ

SELECT 
    FORMAT_TIMESTAMP("%Y", ModifiedDate),
    Name,
    SUM(disc_cost) AS total_cost
FROM (
      SELECT DISTINCT a.*,
      		c.Name,
      		d.DiscountPct, 
      		d.Type,
      		a.OrderQty * d.DiscountPct * UnitPrice AS disc_cost 
      FROM `adventureworks2019.Sales.SalesOrderDetail` a
      LEFT JOIN `adventureworks2019.Production.Product` b 
      	ON a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c 
      	ON cast(b.ProductSubcategoryID AS int) = c.ProductSubcategoryID
      LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d 
      	ON a.SpecialOfferID = d.SpecialOfferID
      WHERE lower(d.Type) like '%seasonal discount%' 
)
GROUP BY 1,2;


---------Query 05: Retention rate of Customer in 2014 WITH status of Successfully Shipped (Cohort Analysis) 
		-- Tỷ lệ giữ chân khách hàng năm 2014 với trạng thái Đã giao hàng thành công (Phân tích đoàn hệ)
WITH 
info AS (
  SELECT
    EXTRACT(month FROM date(ModifiedDate)) AS month_order,
    EXTRACT(year FROM date(ModifiedDate)) AS yr,
    CustomerID,
    COUNT(DISTINCT SalesOrderID) AS sales_cnt
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE EXTRACT(year FROM date(ModifiedDate)) = 2014
    AND Status = 5
  GROUP BY 1,2,3
),

row_num AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY month_order ASC) AS row_nb
  FROM info
),

first_order AS(
  SELECT 
    DISTINCT month_order AS month_join,
    yr,
    CustomerID,
  FROM row_num
  WHERE row_nb = 1
),

all_join AS(
  SELECT 
    DISTINCT a.month_order,
    a.yr,
    a.CustomerID,
    b.month_join,
    CONCAT('M-',a.month_order-b.month_join) AS month_diff
  FROM info AS a
  LEFT JOIN first_order AS b
  USING(CustomerID)
  ORDER BY 3
)

SELECT
  DISTINCT month_join,
  month_diff,
  COUNT(DISTINCT CustomerID) AS customer_cnt
FROM all_join
GROUP BY month_join, month_diff
ORDER BY month_join;


---------Query 06: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
		-- Xu hướng mức tồn kho & % chênh lệch tháng qua tháng theo tất cả sản phẩm trong năm 2011. Tỷ lệ %gr là null thì = 0.
		-- Làm tròn đến 1 chữ số thập phân

WITH raw AS (
	SELECT
  	EXTRACT(month FROM a.ModifiedDate) AS mth, 
    	EXTRACT(year FROM a.ModifiedDate) AS yr, 
    	b.Name,
    	SUM(StockedQty) AS stock_qty
	FROM `adventureworks2019.Production.WorkOrder` a
	LEFT JOIN `adventureworks2019.Production.Product` b 
		ON a.ProductID = b.ProductID
	WHERE FORMAT_TIMESTAMP("%Y", a.ModifiedDate) = '2011'
	GROUP BY 1,2,3
	ORDER BY 1 DESC 
)

SELECT DISTINCT Name,
      mth,
      yr,
      stock_qty,
      stock_prv, 
      COALESCE(ROUND((stock_qty /stock_prv - 1)*100,1),0) AS diff
FROM (
	SELECT *, LEAD (stock_qty) OVER (PARTITION BY Name ORDER BY mth DESC) AS stock_prv
    	FROM raw
)
ORDER BY 1 ASC, 2 DESC;


---------Query 07: Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month DESC, ratio DESC. Round Ratio to 1 decimal
		-- Tính tỷ lệ tồn kho/doanh thu năm 2011 theo tên sản phẩm, theo tháng. Sắp xếp kết quả tháng DESC, tỷ lệ DESC. Tỷ lệ làm tròn đến 1 thập phân

WITH a AS(
  SELECT
    EXTRACT(month FROM date(t2.ModifiedDate)) AS mth,
    EXTRACT(year FROM date(t2.ModifiedDate)) AS yr,
    ProductID,
    t1.Name,
    SUM(OrderQty) AS sales_cnt
  FROM `adventureworks2019.Production.Product` AS t1
  LEFT JOIN `adventureworks2019.Sales.SalesOrderDetail` AS t2
  USING(ProductID)
  WHERE EXTRACT(year FROM date(t2.ModifiedDate)) = 2011
  GROUP BY mth, yr, ProductID, Name
),

b AS(
  SELECT 
    ProductID,
    EXTRACT(month FROM date(t3.ModifiedDate)) AS mth,
    SUM(StockedQty) AS stock_cnt
  FROM `adventureworks2019.Production.Product` AS t1
  LEFT JOIN `adventureworks2019.Production.WorkOrder` AS t3
  USING(ProductID)
  WHERE EXTRACT(year FROM date(t3.ModifiedDate)) = 2011
  GROUP BY ProductID, mth)
	
SELECT
  a.mth,
  a.yr,
  a.ProductID,
  a.Name,
  a.sales_cnt AS sales,
  b.stock_cnt AS stock,
  ROUND(stock_cnt/sales_cnt,1) AS ratio
FROM a
LEFT JOIN b
USING(ProductID,mth)
WHERE sales_cnt is not null
	AND stock_cnt is not null
ORDER BY mth DESC, ratio DESC;


---------Query 08: No of order and value at Pending status in 2014
		-- Số lượng đơn hàng và giá trị ở trạng thái Chờ xử lý năm 2014

SELECT 
	EXTRACT(year FROM date(ModifiedDate)) AS yr,
	1 AS Status,
	COUNT(DISTINCT PurchaseOrderID) AS order_cnt,
	SUM(TotalDue) AS value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE Status = 1
	AND EXTRACT(year FROM date(ModifiedDate)) = 2014
GROUP BY yr;

--The end--
