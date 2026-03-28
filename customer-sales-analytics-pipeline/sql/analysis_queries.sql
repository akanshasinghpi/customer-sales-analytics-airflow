-- Top 5 Customers by Revenue
SELECT Customer_ID, SUM(Revenue) AS total_revenue
FROM sales
GROUP BY Customer_ID
ORDER BY total_revenue DESC
LIMIT 5;


-- Revenue by Region
SELECT Region, SUM(Revenue) AS revenue
FROM sales
GROUP BY Region
ORDER BY revenue DESC;


-- Monthly Sales Trend
SELECT Month, SUM(Revenue) AS monthly_revenue
FROM sales
GROUP BY Month
ORDER BY Month;


-- Top Selling Products
SELECT Product, SUM(Revenue) AS product_revenue
FROM sales
GROUP BY Product
ORDER BY product_revenue DESC
LIMIT 10;
