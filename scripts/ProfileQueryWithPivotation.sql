-- Utiliza un CTE (Common Table Expression) para obtener los datos base
WITH BaseData AS (
    SELECT
        sp.[Date],
        s.Symbol,
        -- Selecciona las 5 métricas que queremos pivotar
        CAST(sp.[Open] AS FLOAT) AS [Open], -- Asegúrate que los tipos de dato sean compatibles para UNPIVOT
        CAST(sp.[Close] AS FLOAT) AS [Close],
        CAST(sp.[Low] AS FLOAT) AS [Low],
        CAST(sp.[High] AS FLOAT) AS [High],
        CAST(sp.Volume AS FLOAT) AS Volume -- Podría ser BIGINT también, pero FLOAT funciona para UNPIVOT
    FROM
        AVdata.StockPrices sp
    INNER JOIN
        Metadata.Symbols s ON sp.SymbolID = s.SymbolID
    WHERE
        s.Symbol IN ('AAPL', 'NVDA') -- Filtra símbolos de interés
),
-- Paso 1: UNPIVOT de las métricas
-- Convierte las columnas Open, Close, Low, High, Volume en filas.
-- Cada fila original se convierte en 5 filas (una por métrica).
UnpivotedData AS (
    SELECT
        [Date],
        Symbol,
        MetricType, -- Nombre de la métrica original (Open, Close, etc.)
        MetricValue -- Valor de la métrica
    FROM BaseData
    UNPIVOT (
        MetricValue -- Nueva columna que contendrá los valores de las métricas
        FOR MetricType IN ([Open], [Close], [Low], [High], [Volume]) -- Columnas originales a des-pivotar
    ) AS unpvt -- Alias para la operación UNPIVOT
)
-- Paso 2: PIVOT de los datos des-pivotados
-- Ahora pivotamos usando una combinación de MetricType y Symbol para los nombres de columna.
SELECT
    [Date],
    -- Lista explícita de todas las columnas finales deseadas
    [Open_AAPL], [Close_AAPL], [Low_AAPL], [High_AAPL], [Volume_AAPL],
    [Open_NVDA], [Close_NVDA], [Low_NVDA], [High_NVDA], [Volume_NVDA]
FROM (
    -- Preparamos los datos para el PIVOT final
    SELECT
        [Date],
        -- Creamos el nombre de columna combinado que PIVOT usará (ej: 'Open_AAPL')
        MetricType + '_' + Symbol AS PivotColumnName,
        MetricValue
    FROM UnpivotedData
) AS SourceForPivot -- Alias para la subconsulta fuente
PIVOT (
    -- Función de agregación (MAX o SUM funcionan, ya que solo hay un valor por Date/PivotColumnName)
    MAX(MetricValue)
    -- La columna cuyos valores se convertirán en las nuevas cabeceras de columna
    FOR PivotColumnName IN (
        -- Lista explícita de todos los nombres de columna que se crearán
        [Open_AAPL], [Close_AAPL], [Low_AAPL], [High_AAPL], [Volume_AAPL],
        [Open_NVDA], [Close_NVDA], [Low_NVDA], [High_NVDA], [Volume_NVDA]
    )
) AS PivotTable -- Alias para la tabla pivotada final
ORDER BY
    [Date] DESC; -- Ordenamos por fecha
