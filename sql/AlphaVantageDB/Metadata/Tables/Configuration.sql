CREATE TABLE [Metadata].[Configuration] (
    [ConfigID]     INT            IDENTITY (1, 1) NOT NULL,
    [ConfigKey]    NVARCHAR (50)  NOT NULL,
    [ConfigValue]  NVARCHAR (500) NOT NULL,
    [Description]  NVARCHAR (200) NULL,
    [CreatedDate]  DATETIME       DEFAULT (getdate()) NULL,
    [ModifiedDate] DATETIME       DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([ConfigID] ASC),
    CONSTRAINT [UQ_ConfigKey] UNIQUE NONCLUSTERED ([ConfigKey] ASC)
);


GO

