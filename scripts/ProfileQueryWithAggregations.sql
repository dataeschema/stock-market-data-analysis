-- Selecciona la fecha y las métricas pivotadas para AAPL y NVDA
SELECT
    sp.[Date],

    -- Columnas para AAPL
    -- Usamos MAX con CASE: si el símbolo es 'AAPL', toma el valor; si no, es NULL.
    -- MAX ignora los NULLs y devuelve el valor encontrado para 'AAPL' en esa fecha.
    MAX(CASE WHEN s.Symbol = 'AAPL' THEN CAST(sp.[Open] AS FLOAT) ELSE NULL END) AS Open_AAPL,
    MAX(CASE WHEN s.Symbol = 'AAPL' THEN CAST(sp.[Close] AS FLOAT) ELSE NULL END) AS Close_AAPL,
    MAX(CASE WHEN s.Symbol = 'AAPL' THEN CAST(sp.[Low] AS FLOAT) ELSE NULL END) AS Low_AAPL,
    MAX(CASE WHEN s.Symbol = 'AAPL' THEN CAST(sp.[High] AS FLOAT) ELSE NULL END) AS High_AAPL,
    MAX(CASE WHEN s.Symbol = 'AAPL' THEN CAST(sp.Volume AS BIGINT) ELSE NULL END) AS Volume_AAPL,

    -- Columnas para NVDA
    -- Repetimos el proceso para 'NVDA'
    MAX(CASE WHEN s.Symbol = 'NVDA' THEN CAST(sp.[Open] AS FLOAT) ELSE NULL END) AS Open_NVDA,
    MAX(CASE WHEN s.Symbol = 'NVDA' THEN CAST(sp.[Close] AS FLOAT) ELSE NULL END) AS Close_NVDA,
    MAX(CASE WHEN s.Symbol = 'NVDA' THEN CAST(sp.[Low] AS FLOAT) ELSE NULL END) AS Low_NVDA,
    MAX(CASE WHEN s.Symbol = 'NVDA' THEN CAST(sp.[High] AS FLOAT) ELSE NULL END) AS High_NVDA,
    MAX(CASE WHEN s.Symbol = 'NVDA' THEN CAST(sp.Volume AS BIGINT) ELSE NULL END) AS Volume_NVDA
FROM
    -- Tabla principal de precios
    AVdata.StockPrices sp
    -- Unimos con la tabla de símbolos (INNER JOIN es suficiente aquí)
    INNER JOIN Metadata.Symbols s ON sp.SymbolID = s.SymbolID
WHERE
    -- Filtramos solo para los símbolos que nos interesan ANTES de agrupar
    s.Symbol IN ('AAPL', 'NVDA')
GROUP BY
    -- Agrupamos por fecha para tener una fila por día
    sp.[Date]
ORDER BY
    -- Ordenamos por fecha (opcional, pero útil)
    sp.[Date] desc;
