CREATE TABLE [Metadata].[DateDim]
(
  [IdFecha] INT NOT NULL PRIMARY KEY,
  [Fecha] DATE NOT NULL,
  [DiaMes] INT NOT NULL,
  [MesDelAno] INT NOT NULL,
  [Ano] INT NOT NULL,
  [NombreMes] VARCHAR(25) NOT NULL,
  [MesAno] VARCHAR(7) NOT NULL,
  [SemanaAno] INT NOT NULL,
  [NombreDia] VARCHAR(25)
)
