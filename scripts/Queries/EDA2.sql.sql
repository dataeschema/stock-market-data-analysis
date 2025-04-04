-- Estructura corregida: Un solo WITH para definir todos los CTEs secuencialmente
WITH
-- Paso 1: CTE para obtener los datos base de precios y símbolos
BaseData AS (
    SELECT
        sp.[Date],
        s.Symbol,
        sp.[Open],
        sp.[Close],
        sp.[Low],
        sp.[High],
        sp.Volume
    FROM
        AVdata.StockPrices sp
    INNER JOIN
        Metadata.Symbols s ON sp.SymbolID = s.SymbolID
    WHERE
        s.Symbol IN ('AAPL', 'NVDA') -- Filtramos solo los símbolos de interés
), -- Coma para separar del siguiente CTE
-- Paso 2: CTE para calcular las características adicionales usando funciones de ventana
FeatureCalculation AS (
    SELECT
        Date,
        Symbol,
        [Open],
        [Close],
        [Low],
        [High],
        Volume,

        -- --- Métricas Mensuales ---
        AVG([Close]) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyAvg_Close,
        STDEV([Close]) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyStdDev_Close,
        AVG(CAST(Volume AS FLOAT)) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyAvg_Volume, -- CAST a FLOAT para AVG/STDEV
        STDEV(CAST(Volume AS FLOAT)) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyStdDev_Volume,

        -- --- Métricas Móviles (Rolling) ---
        AVG([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS MA_20d_Close,
        AVG([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS MA_50d_Close,
        STDEV([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS Volatility_20d_Close,

        -- --- Métricas de Cambio y Lag ---
        LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date) AS PrevDay_Close,
        ([Close] - LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date))
            / NULLIF(LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date), 0) -- Evita división por cero
            AS PctChange_Close_Daily,
        ([High] - [Low]) AS DailyRange,

        -- --- Características de Fecha ---
        DATEPART(weekday, Date) AS DayOfWeek,
        DATEPART(day, Date) AS DayOfMonth,
        DATEPART(month, Date) AS Month

    FROM BaseData -- Usa el CTE anterior
), -- Coma para separar del siguiente CTE
-- Paso 3: CTE para pivotar TODO (métricas base + características calculadas)
EnrichedData AS (
    SELECT
        fc.Date,
        -- --- Columnas AAPL ---
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Open] ELSE NULL END) AS Open_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Close] ELSE NULL END) AS Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Low] ELSE NULL END) AS Low_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[High] ELSE NULL END) AS High_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Volume ELSE NULL END) AS Volume_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyAvg_Close ELSE NULL END) AS MonthlyAvg_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyStdDev_Close ELSE NULL END) AS MonthlyStdDev_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyAvg_Volume ELSE NULL END) AS MonthlyAvg_Volume_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyStdDev_Volume ELSE NULL END) AS MonthlyStdDev_Volume_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MA_20d_Close ELSE NULL END) AS MA_20d_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MA_50d_Close ELSE NULL END) AS MA_50d_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Volatility_20d_Close ELSE NULL END) AS Volatility_20d_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.PrevDay_Close ELSE NULL END) AS PrevDay_Close_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.PctChange_Close_Daily ELSE NULL END) AS PctChange_Close_Daily_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DailyRange ELSE NULL END) AS DailyRange_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DayOfWeek ELSE NULL END) AS DayOfWeek_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DayOfMonth ELSE NULL END) AS DayOfMonth_AAPL,
        MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Month ELSE NULL END) AS Month_AAPL,

        -- --- Columnas NVDA ---
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Open] ELSE NULL END) AS Open_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Close] ELSE NULL END) AS Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Low] ELSE NULL END) AS Low_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[High] ELSE NULL END) AS High_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Volume ELSE NULL END) AS Volume_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyAvg_Close ELSE NULL END) AS MonthlyAvg_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyStdDev_Close ELSE NULL END) AS MonthlyStdDev_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyAvg_Volume ELSE NULL END) AS MonthlyAvg_Volume_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyStdDev_Volume ELSE NULL END) AS MonthlyStdDev_Volume_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MA_20d_Close ELSE NULL END) AS MA_20d_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MA_50d_Close ELSE NULL END) AS MA_50d_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Volatility_20d_Close ELSE NULL END) AS Volatility_20d_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.PrevDay_Close ELSE NULL END) AS PrevDay_Close_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.PctChange_Close_Daily ELSE NULL END) AS PctChange_Close_Daily_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DailyRange ELSE NULL END) AS DailyRange_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DayOfWeek ELSE NULL END) AS DayOfWeek_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DayOfMonth ELSE NULL END) AS DayOfMonth_NVDA,
        MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Month ELSE NULL END) AS Month_NVDA

    FROM FeatureCalculation fc -- Usa el CTE anterior
    GROUP BY
        fc.Date -- Agrupamos por fecha para obtener una fila por día
) -- Fin de la definición de CTEs

-- Paso final: Seleccionar datos y calcular IQR móvil usando OUTER APPLY sobre EnrichedData
SELECT
    e1.*, -- Selecciona todas las columnas del CTE EnrichedData

    -- --- Flags de Outlier para AAPL ---
    -- Flag para Close_AAPL
    CASE
        WHEN e1.Close_AAPL < (iqr_aapl.Q1_Close_AAPL - 1.5 * (iqr_aapl.Q3_Close_AAPL - iqr_aapl.Q1_Close_AAPL))
          OR e1.Close_AAPL > (iqr_aapl.Q3_Close_AAPL + 1.5 * (iqr_aapl.Q3_Close_AAPL - iqr_aapl.Q1_Close_AAPL))
        THEN 1 ELSE 0
    END AS IsOutlier_IQR_Close_AAPL,
    -- Flag para Volume_AAPL
    CASE
        WHEN CAST(e1.Volume_AAPL AS FLOAT) < (iqr_aapl.Q1_Volume_AAPL - 1.5 * (iqr_aapl.Q3_Volume_AAPL - iqr_aapl.Q1_Volume_AAPL))
          OR CAST(e1.Volume_AAPL AS FLOAT) > (iqr_aapl.Q3_Volume_AAPL + 1.5 * (iqr_aapl.Q3_Volume_AAPL - iqr_aapl.Q1_Volume_AAPL))
        THEN 1 ELSE 0
    END AS IsOutlier_IQR_Volume_AAPL,

    -- --- Flags de Outlier para NVDA ---
     -- Flag para Close_NVDA
    CASE
        WHEN e1.Close_NVDA < (iqr_nvda.Q1_Close_NVDA - 1.5 * (iqr_nvda.Q3_Close_NVDA - iqr_nvda.Q1_Close_NVDA))
          OR e1.Close_NVDA > (iqr_nvda.Q3_Close_NVDA + 1.5 * (iqr_nvda.Q3_Close_NVDA - iqr_nvda.Q1_Close_NVDA))
        THEN 1 ELSE 0
    END AS IsOutlier_IQR_Close_NVDA,
    -- Flag para Volume_NVDA
    CASE
        WHEN CAST(e1.Volume_NVDA AS FLOAT) < (iqr_nvda.Q1_Volume_NVDA - 1.5 * (iqr_nvda.Q3_Volume_NVDA - iqr_nvda.Q1_Volume_NVDA))
          OR CAST(e1.Volume_NVDA AS FLOAT) > (iqr_nvda.Q3_Volume_NVDA + 1.5 * (iqr_nvda.Q3_Volume_NVDA - iqr_nvda.Q1_Volume_NVDA))
        THEN 1 ELSE 0
    END AS IsOutlier_IQR_Volume_NVDA

FROM
    EnrichedData e1 -- Alias 'e1' para la tabla principal (cada fila que estamos procesando)

    -- Calcular Q1/Q3 móvil para AAPL
    OUTER APPLY (
        SELECT
            -- Calcula Q1 y Q3 para la ventana definida por el WHERE del APPLY
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Close_AAPL) OVER () AS Q1_Close_AAPL,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Close_AAPL) OVER () AS Q3_Close_AAPL,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(Volume_AAPL AS FLOAT)) OVER () AS Q1_Volume_AAPL,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(Volume_AAPL AS FLOAT)) OVER () AS Q3_Volume_AAPL
        FROM EnrichedData e2 -- Alias 'e2' para la subconsulta que mira la ventana
        WHERE e2.Date BETWEEN DATEADD(day, -89, e1.Date) AND e1.Date -- Define la ventana de 90 días hacia atrás desde la fila e1
        -- El OVER() vacío es correcto aquí porque la ventana la define el WHERE del APPLY
    ) AS iqr_aapl -- Alias para los resultados del APPLY de AAPL

    -- Calcular Q1/Q3 móvil para NVDA
    OUTER APPLY (
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Close_NVDA) OVER () AS Q1_Close_NVDA,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Close_NVDA) OVER () AS Q3_Close_NVDA,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(Volume_NVDA AS FLOAT)) OVER () AS Q1_Volume_NVDA,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(Volume_NVDA AS FLOAT)) OVER () AS Q3_Volume_NVDA
        FROM EnrichedData e2
        WHERE e2.Date BETWEEN DATEADD(day, -89, e1.Date) AND e1.Date
    ) AS iqr_nvda -- Alias para los resultados del APPLY de NVDA

ORDER BY
    e1.Date; -- Ordenar resultado final
