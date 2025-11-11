USE salarkhan;

-- Q1
SELECT
    dd.month_number AS month,
    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS is_weekend,
    dp.product_id,
    dp.product_category,
    SUM(fs.total_amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE dd.year = 2017
GROUP BY dd.month_number, is_weekend, dp.product_id, dp.product_category
ORDER BY dd.month_number, is_weekend, total_revenue DESC
LIMIT 5;

-- Q2
SELECT
    dc.gender,
    dc.age_range,
    dc.city_category,
    SUM(fs.total_amount) AS total_purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.gender, dc.age_range, dc.city_category
ORDER BY dc.gender, dc.age_range, dc.city_category;

-- Q3
SELECT
    dc.occupation_code,
    dp.product_category,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dc.occupation_code, dp.product_category
ORDER BY dc.occupation_code, total_sales DESC;

-- Q4
SELECT
    dd.year,
    dd.quarter_number,
    dc.gender,
    dc.age_range,
    SUM(fs.total_amount) AS total_purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY dd.year, dd.quarter_number, dc.gender, dc.age_range
ORDER BY dd.year, dd.quarter_number, dc.gender, dc.age_range;

-- Q5
SELECT product_category, occupation_code, total_sales
FROM (
    SELECT
        dp.product_category,
        dc.occupation_code,
        SUM(fs.total_amount) AS total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY dp.product_category
            ORDER BY SUM(fs.total_amount) DESC
        ) AS rn
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dp.product_category, dc.occupation_code
) occupation_sales
WHERE rn <= 5
ORDER BY product_category, total_sales DESC;

-- Q6
SELECT
    dd.year,
    dd.month_number,
    dc.city_category,
    dc.marital_status,
    SUM(fs.total_amount) AS total_purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE dd.date_id >= (
    SELECT DATE_SUB(MAX(date_id), INTERVAL 6 MONTH) 
    FROM dim_date
)
GROUP BY dd.year, dd.month_number, dc.city_category, dc.marital_status
ORDER BY dd.year, dd.month_number, dc.city_category, dc.marital_status;

-- Q7
SELECT
    dc.stay_in_city_years,
    dc.gender,
    AVG(fs.total_amount) AS avg_purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.stay_in_city_years, dc.gender
ORDER BY dc.stay_in_city_years, dc.gender;

-- Q8
SELECT product_category, city_category, revenue
FROM (
    SELECT
        dp.product_category,
        dc.city_category,
        SUM(fs.total_amount) AS revenue,
        ROW_NUMBER() OVER (
            PARTITION BY dp.product_category
            ORDER BY SUM(fs.total_amount) DESC
        ) AS rn
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dp.product_category, dc.city_category
) ranked_cities
WHERE rn <= 5
ORDER BY product_category, revenue DESC;

-- Q9
SELECT
    product_category,
    year,
    month_number,
    revenue,
    CASE
        WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
        ELSE ((revenue - prev_revenue) / prev_revenue) * 100
    END AS growth_percentage
FROM (
    SELECT
        dp.product_category,
        dd.year,
        dd.month_number,
        SUM(fs.total_amount) AS revenue,
        LAG(SUM(fs.total_amount)) OVER (
            PARTITION BY dp.product_category, dd.year 
            ORDER BY dd.month_number
        ) AS prev_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    GROUP BY dp.product_category, dd.year, dd.month_number
) monthly_sales
ORDER BY product_category, year, month_number;

-- Q10
SELECT
    dd.year,
    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_group,
    dc.age_range,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY dd.year, day_group, dc.age_range
ORDER BY dd.year, day_group, dc.age_range;

-- Q11
SELECT product_id, product_category, year, month_number, month_name, day_group, revenue
FROM (
    SELECT
        dp.product_id,
        dp.product_category,
        dd.year,
        dd.month_number,
        dd.month_name,
        CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_group,
        SUM(fs.total_amount) AS revenue,
        ROW_NUMBER() OVER (
            PARTITION BY dd.year, dd.month_number, CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY SUM(fs.total_amount) DESC
        ) AS rn
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    WHERE dd.year = 2017
    GROUP BY dp.product_id, dp.product_category, dd.year, dd.month_number, dd.month_name, day_group
) ranked_products
WHERE rn <= 5
ORDER BY year, month_number, day_group, revenue DESC;

-- Q12
SELECT
    store_name,
    quarter_number,
    revenue,
    LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter_number) AS prev_revenue,
    CASE
        WHEN LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter_number) IS NULL THEN NULL
        WHEN LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter_number) = 0 THEN NULL
        ELSE (
            (revenue - LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter_number)) /
            LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter_number)
        ) * 100
    END AS growth_rate
FROM (
    SELECT
        ds.store_name,
        dd.year,
        dd.quarter_number,
        SUM(fs.total_amount) AS revenue
    FROM fact_sales fs
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    WHERE dd.year = 2017
    GROUP BY ds.store_name, dd.year, dd.quarter_number
) store_quarter
ORDER BY store_name, quarter_number;

-- Q13
SELECT
    ds.store_name,
    dsu.supplier_name,
    dp.product_id,
    dp.product_category,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_store ds ON fs.store_id = ds.store_id
JOIN dim_supplier dsu ON fs.supplier_id = dsu.supplier_id
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY ds.store_name, dsu.supplier_name, dp.product_id, dp.product_category
ORDER BY ds.store_name, dsu.supplier_name, total_sales DESC;

-- Q14
SELECT
    dp.product_id,
    dp.product_category,
    CASE
        WHEN dd.month_number IN (12, 1, 2) THEN 'Winter'
        WHEN dd.month_number IN (3, 4, 5) THEN 'Spring'
        WHEN dd.month_number IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY dp.product_id, dp.product_category, season
ORDER BY dp.product_id, season;

-- Q15
SELECT
    store_name,
    supplier_name,
    year,
    month_number,
    revenue,
    CASE
        WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
        ELSE ((revenue - prev_revenue) / prev_revenue) * 100
    END AS volatility_pct
FROM (
    SELECT
        ds.store_name,
        dsu.supplier_name,
        dd.year,
        dd.month_number,
        SUM(fs.total_amount) AS revenue,
        LAG(SUM(fs.total_amount)) OVER (
            PARTITION BY ds.store_name, dsu.supplier_name
            ORDER BY dd.year, dd.month_number
        ) AS prev_revenue
    FROM fact_sales fs
    JOIN dim_store ds ON fs.store_id = ds.store_id
    JOIN dim_supplier dsu ON fs.supplier_id = dsu.supplier_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    GROUP BY ds.store_name, dsu.supplier_name, dd.year, dd.month_number
) delta
ORDER BY store_name, supplier_name, year, month_number;

-- Q16
SELECT 
    LEAST(fs1.product_id, fs2.product_id) AS product_a,
    GREATEST(fs1.product_id, fs2.product_id) AS product_b,
    COUNT(DISTINCT fs1.customer_id) AS customer_count
FROM fact_sales fs1
JOIN fact_sales fs2 
    ON fs1.customer_id = fs2.customer_id
   AND fs1.product_id < fs2.product_id
GROUP BY product_a, product_b
ORDER BY customer_count DESC
LIMIT 5;

-- Q17
SELECT
    ds.store_name,
    dsu.supplier_name,
    dp.product_id,
    dd.year,
    SUM(fs.total_amount) AS revenue
FROM fact_sales fs
JOIN dim_store ds ON fs.store_id = ds.store_id
JOIN dim_supplier dsu ON fs.supplier_id = dsu.supplier_id
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY ds.store_name, dsu.supplier_name, dp.product_id, dd.year WITH ROLLUP
ORDER BY ds.store_name, dsu.supplier_name, dp.product_id, dd.year;

-- Q18
SELECT
    product_id,
    SUM(CASE WHEN half = 'H1' THEN revenue ELSE 0 END) AS revenue_h1,
    SUM(CASE WHEN half = 'H2' THEN revenue ELSE 0 END) AS revenue_h2,
    SUM(revenue) AS revenue_total,
    SUM(CASE WHEN half = 'H1' THEN quantity ELSE 0 END) AS qty_h1,
    SUM(CASE WHEN half = 'H2' THEN quantity ELSE 0 END) AS qty_h2,
    SUM(quantity) AS qty_total
FROM (
    SELECT
        dp.product_id,
        CASE WHEN dd.month_number <= 6 THEN 'H1' ELSE 'H2' END AS half,
        SUM(fs.total_amount) AS revenue,
        SUM(fs.quantity) AS quantity
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    GROUP BY dp.product_id, half
) half_year
GROUP BY product_id
ORDER BY product_id;

-- Q19
SELECT
    d.product_id,
    d.date_id,
    d.daily_revenue,
    a.avg_revenue,
    CASE WHEN d.daily_revenue >= 2 * a.avg_revenue THEN 'Outlier' ELSE 'Normal' END AS status
FROM (
    SELECT
        dp.product_id,
        dd.date_id,
        SUM(fs.total_amount) AS daily_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_date dd ON fs.date_id = dd.date_id
    GROUP BY dp.product_id, dd.date_id
) d
JOIN (
    SELECT
        dp.product_id,
        AVG(daily_rev) AS avg_revenue
    FROM (
        SELECT
            dp.product_id,
            dd.date_id,
            SUM(fs.total_amount) AS daily_rev
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_id = dp.product_id
        JOIN dim_date dd ON fs.date_id = dd.date_id
        GROUP BY dp.product_id, dd.date_id
    ) daily
    JOIN dim_product dp ON daily.product_id = dp.product_id
    GROUP BY dp.product_id
) a ON d.product_id = a.product_id
WHERE d.daily_revenue >= 2 * a.avg_revenue
ORDER BY d.product_id, d.date_id;

-- Q20
SELECT
    ds.store_name,
    dd.year,
    dd.quarter_number,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_store ds ON fs.store_id = ds.store_id
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY ds.store_name, dd.year, dd.quarter_number
ORDER BY ds.store_name, dd.year, dd.quarter_number;
