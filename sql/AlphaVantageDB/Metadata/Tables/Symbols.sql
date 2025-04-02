CREATE TABLE [Metadata].[Symbols] (
    [SymbolID]    INT            IDENTITY (1, 1) NOT NULL,
    [Symbol]      NVARCHAR (20)  NOT NULL,
    [CompanyName] NVARCHAR (100) NOT NULL,
    [IsActive]    BIT            DEFAULT ((1)) NULL,
    [CreatedDate] DATETIME       DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([SymbolID] ASC)
);


GO

