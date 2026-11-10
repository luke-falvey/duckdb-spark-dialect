-- statistical aggregates over one row: NULL or NaN (legacy); literals via VALUES, grouping, NULLs, windows, negative control

SELECT var_samp(x), stddev_samp(x), corr(x, x) FROM VALUES (1) t(x);

SELECT variance(x), stddev(x), std(x), covar_samp(x, x) FROM VALUES (1) t(x);

SELECT var_samp(x), stddev_samp(x) FROM VALUES (1), (3) t(x);

SELECT var_samp(x), stddev_samp(x), covar_samp(x, x), corr(x, x) FROM VALUES (CAST(NULL AS INT)) t(x);

SELECT var_samp(x), stddev_samp(x) FROM VALUES (1), (CAST(NULL AS INT)) t(x);

SELECT k, var_samp(x), stddev_samp(x), covar_samp(x, y), corr(x, y) FROM VALUES (1, 1, 2), (1, 2, 3), (2, 5, 1) t(k, x, y) GROUP BY k ORDER BY k;

SELECT covar_samp(x, y), corr(x, y) FROM VALUES (1, 2), (CAST(NULL AS INT), 5) t(x, y);

SELECT var_pop(x), stddev_pop(x), covar_pop(x, x) FROM VALUES (1) t(x);

SELECT var_samp(x) OVER () FROM VALUES (1) t(x);

SELECT var_samp(CAST(x AS DECIMAL(5, 2))), var_samp(CAST(x AS DOUBLE)) FROM VALUES (1) t(x);
