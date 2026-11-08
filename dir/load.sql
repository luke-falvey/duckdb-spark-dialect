COPY ctas_probe FROM 'dir/ctas_probe.csv' (FORMAT 'csv', allow_quoted_nulls false, quote '"', delimiter ',', header 1);
COPY ins_probe FROM 'dir/ins_probe.csv' (FORMAT 'csv', allow_quoted_nulls false, quote '"', delimiter ',', header 1);
