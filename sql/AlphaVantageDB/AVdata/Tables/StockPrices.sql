CREATE TABLE [AVdata].[StockPrices] (
    [PriceID]       INT             IDENTITY (1, 1) NOT NULL,
    [SymbolID]      INT             NOT NULL,
    [Date]          DATE            NOT NULL,
    [Open]          DECIMAL (18, 6) NULL,
    [High]          DECIMAL (18, 6) NULL,
    [Low]           DECIMAL (18, 6) NULL,
    [Close]         DECIMAL (18, 6) NULL,
    [Volume]        BIGINT          NULL,
    [CreatedDate]   DATETIME        DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([PriceID] ASC),
    CONSTRAINT [FK_StockPrices_Symbols] FOREIGN KEY ([SymbolID]) REFERENCES [Metadata].[Symbols] ([SymbolID])
);


GO

CREATE NONCLUSTERED INDEX [IX_StockPrices_SymbolDate]
    ON [AVdata].[StockPrices]([SymbolID] ASC, [Date] ASC);


GO

