SET LANGUAGE Spanish;
TRUNCATE TABLE Metadata.DateDim;
WITH DATOS AS(
    SELECT 
        CONVERT(DATE,'01/01/1999', 105) AS fecha
    
    UNION ALL

    SELECT 
        DATEADD(DAY,1,fecha) AS fecha
    FROM
        DATOS
    WHERE
        fecha < CONVERT(DATE,'31/12/2025', 105)
)
INSERT INTO Metadata.DateDim (IdFecha, Fecha, DiaMes, MesDelAno, Ano, NombreMes, MesAno, SemanaAno, NombreDia) 
SELECT 
    CAST(CONVERT(VARCHAR(8), fecha, 112) AS INT) as IdFecha,
    Fecha,
    DAY(fecha) as DiaMes,
    MONTH(fecha) as MesDelAño,
    YEAR(fecha) as Año,
    DATENAME(mm,fecha) as nombremes,
    CAST(MONTH(fecha) AS VARCHAR(2)) + '/' + CAST(YEAR(fecha) AS VARCHAR(4)) as MesAño,
    DATEPART(WEEK, fecha) as SemanaAno,
    DATENAME(WEEKDAY,fecha) as NombreDia
FROM 
    DATOS
OPTION (MAXRECURSION 0)


