SELECT
    dp.category AS category,
    dp.name AS name,
    dp.code AS code,
    '' AS brand,
    SUM(fil.quantity) AS quantity,
    SUM(fil.price_original * fil.quantity) AS gross,
    SUM(fil.net_sales) AS sales,
    SUM(fil.tax) AS tax,
    SUM(fil.net_sales + fil.tax) AS sales_plus_tax,
    COALESCE(SUM(fi.available), 0) AS units_in_stock,
    COALESCE(SUM(dv.unit_price * fil.quantity), 0) AS unit_cost,
    SUM(fil.net_sales) - SUM(COALESCE(dv.unit_price, 0) * fil.quantity) AS profit,
    dp.product_id AS id,
    SPLIT_PART(dp.name, ' ', 1) AS others
FROM fact_invoice_line fil
LEFT JOIN dim_variant dv ON fil.variant_id = dv.variant_id
LEFT JOIN dim_product dp ON dv.product_id = dp.product_id
LEFT JOIN dim_variant_data dvd ON fil.variant_id = dvd.variant_id
LEFT JOIN fact_inventory fi ON dv.code = fi.variant_code
GROUP BY dp.category, dp.name, dp.code, dp.product_id
ORDER BY sales DESC
LIMIT 20;