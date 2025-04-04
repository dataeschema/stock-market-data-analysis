-- Paso 1: CTE para obtener los datos base de precios y símbolos
WITH BaseData AS (
    SELECT
        sp.[Date],
        s.Symbol,
        CAST(sp.[Open] AS float) AS [Open], -- Aseguramos que los tipos sean correctos
        CAST(sp.[Close] AS float) AS [Close],
        CAST(sp.[Low] AS float) AS [Low],
        CAST(sp.[High] AS float) AS [High],
        CAST(sp.Volume AS BIGINT) AS VOLUME
    FROM
        AVdata.StockPrices sp
    INNER JOIN
        Metadata.Symbols s ON sp.SymbolID = s.SymbolID
    WHERE
        s.Symbol IN ('AAPL', 'NVDA') -- Filtramos solo los símbolos de interés
),
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
        -- Media mensual del precio de cierre
        AVG([Close]) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyAvg_Close,
        -- Desviación estándar mensual del precio de cierre
        STDEV([Close]) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyStdDev_Close,
        -- Media mensual del volumen
        AVG(CAST(Volume AS FLOAT)) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyAvg_Volume, -- CAST a FLOAT para AVG/STDEV
        -- Desviación estándar mensual del volumen
        STDEV(CAST(Volume AS FLOAT)) OVER (PARTITION BY Symbol, YEAR(Date), MONTH(Date)) AS MonthlyStdDev_Volume,

        -- --- Métricas Móviles (Rolling) ---
        -- Media móvil de 20 días del cierre
        AVG([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS MA_20d_Close,
        -- Media móvil de 50 días del cierre
        AVG([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS MA_50d_Close,
        -- Volatilidad (Desviación estándar móvil de 20 días del cierre)
        STDEV([Close]) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS Volatility_20d_Close,

        -- --- Métricas de Cambio y Lag ---
        -- Precio de cierre del día anterior
        LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date) AS PrevDay_Close,
        -- Cambio porcentual del cierre respecto al día anterior
        ([Close] - LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date))
            / NULLIF(LAG([Close], 1, NULL) OVER (PARTITION BY Symbol ORDER BY Date), 0) -- Evita división por cero
            AS PctChange_Close_Daily,
        -- Rango diario (High - Low)
        ([High] - [Low]) AS DailyRange,

        -- --- Características de Fecha ---
        -- Día de la semana (1=Domingo, 7=Sábado - depende de @@DATEFIRST)
        DATEPART(weekday, Date) AS DayOfWeek,
        -- Día del mes
        DATEPART(day, Date) AS DayOfMonth,
        -- Mes
        DATEPART(month, Date) AS Month

    FROM BaseData
)
-- Paso 3: Pivotar TODO (métricas base + características calculadas) usando agregación condicional
SELECT
    fc.Date,

    -- --- Columnas AAPL ---
    -- Base
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Open] ELSE NULL END) AS Open_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Close] ELSE NULL END) AS Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[Low] ELSE NULL END) AS Low_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.[High] ELSE NULL END) AS High_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Volume ELSE NULL END) AS Volume_AAPL,
    -- Mensuales
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyAvg_Close ELSE NULL END) AS MonthlyAvg_Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyStdDev_Close ELSE NULL END) AS MonthlyStdDev_Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyAvg_Volume ELSE NULL END) AS MonthlyAvg_Volume_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MonthlyStdDev_Volume ELSE NULL END) AS MonthlyStdDev_Volume_AAPL,
    -- Móviles
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MA_20d_Close ELSE NULL END) AS MA_20d_Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.MA_50d_Close ELSE NULL END) AS MA_50d_Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Volatility_20d_Close ELSE NULL END) AS Volatility_20d_Close_AAPL,
    -- Cambio y Lag
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.PrevDay_Close ELSE NULL END) AS PrevDay_Close_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.PctChange_Close_Daily ELSE NULL END) AS PctChange_Close_Daily_AAPL,
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DailyRange ELSE NULL END) AS DailyRange_AAPL,
    -- Fecha (Estas serían iguales para ambas acciones en el mismo día, pero las pivotamos por completitud)
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DayOfWeek ELSE NULL END) AS DayOfWeek_AAPL, -- Podría tomarse fuera del MAX(CASE...)
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.DayOfMonth ELSE NULL END) AS DayOfMonth_AAPL, -- Ídem
    MAX(CASE WHEN fc.Symbol = 'AAPL' THEN fc.Month ELSE NULL END) AS Month_AAPL, -- Ídem

    -- --- Columnas NVDA ---
    -- Base
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Open] ELSE NULL END) AS Open_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Close] ELSE NULL END) AS Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[Low] ELSE NULL END) AS Low_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.[High] ELSE NULL END) AS High_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Volume ELSE NULL END) AS Volume_NVDA,
    -- Mensuales
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyAvg_Close ELSE NULL END) AS MonthlyAvg_Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyStdDev_Close ELSE NULL END) AS MonthlyStdDev_Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyAvg_Volume ELSE NULL END) AS MonthlyAvg_Volume_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MonthlyStdDev_Volume ELSE NULL END) AS MonthlyStdDev_Volume_NVDA,
    -- Móviles
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MA_20d_Close ELSE NULL END) AS MA_20d_Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.MA_50d_Close ELSE NULL END) AS MA_50d_Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Volatility_20d_Close ELSE NULL END) AS Volatility_20d_Close_NVDA,
    -- Cambio y Lag
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.PrevDay_Close ELSE NULL END) AS PrevDay_Close_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.PctChange_Close_Daily ELSE NULL END) AS PctChange_Close_Daily_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DailyRange ELSE NULL END) AS DailyRange_NVDA,
    -- Fecha
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DayOfWeek ELSE NULL END) AS DayOfWeek_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.DayOfMonth ELSE NULL END) AS DayOfMonth_NVDA,
    MAX(CASE WHEN fc.Symbol = 'NVDA' THEN fc.Month ELSE NULL END) AS Month_NVDA

FROM FeatureCalculation fc
GROUP BY
    fc.Date -- Agrupamos por fecha para obtener una fila por día
ORDER BY
    fc.Date; -- Ordenamos el resultado final
