CREATE TABLE [Metadata].[Downloads] (
    [DownloadID]       INT           IDENTITY (1, 1) NOT NULL,
    [SymbolID]         INT           NOT NULL,
    [LastDownloadDate] DATETIME      NULL,
    [StartDate]        DATE          NULL,
    [EndDate]          DATE          NULL,
    [Status]           NVARCHAR (20) DEFAULT ('Pending') NULL,
    [CreatedDate]      DATETIME      DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([DownloadID] ASC),
    CONSTRAINT [FK_Downloads_Symbols] FOREIGN KEY ([SymbolID]) REFERENCES [Metadata].[Symbols] ([SymbolID])
);


GO

