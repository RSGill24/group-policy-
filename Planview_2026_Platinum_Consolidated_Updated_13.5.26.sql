/*
================================================================================
  Planview2026PlatinumConsolidatedUpdated
================================================================================
  
  INPUTS (loaded by Python Step 1):
      [{@InputSchema}].[{@Stem}_Initiatives]
      [{@InputSchema}].[{@Stem}_Epics]

  PARAMETERS:
      @RunID           nvarchar(50)    e.g. '20250501_143022'
      @InputSchema     nvarchar(128)   e.g. 'input_20250501_143022'
      @Stem            nvarchar(200)   e.g. 'Planview_Prod_Data_Extract_05_01'
      @NRB_Field       nvarchar(256)   NRB column SQL-safe name
      @NRB_Threshold_M float           NRB cutoff in millions e.g. 10

  RETURNS (6 result sets read by Python in order):
      1 — Classified Initiatives rows  (all original cols + 5 classification cols)
      2 — Classified Epics rows        (all original cols + 9 classification cols)
      3 — Scalar counts: removed_in, removed_ep, new_id_count
      4 — changes_in  (col, cnt)
      5 — changes_ep  (col, cnt)
      6 — Deleted Initiative rows (Stage A: L0 or B: SL1)

  DEPLOYMENT: Run once in SSMS — no parameters needed at deployment time.
================================================================================
*/

CREATE OR ALTER PROCEDURE dbo.Planview2026PlatinumConsolidatedUpdated
    @RunID           nvarchar(50),
    @InputSchema     nvarchar(128),
    @Stem            nvarchar(200),
    @NRB_Field       nvarchar(256),
    @NRB_Threshold_M float = 10
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @tbl_init   nvarchar(500),
        @tbl_epic   nvarchar(500),
        @tbl_iwork  nvarchar(500),
        @tbl_ework  nvarchar(500),
        @sql        nvarchar(MAX),
        @removed_in int = 0;

    SET @tbl_init  = QUOTENAME(@InputSchema) + '.' + QUOTENAME(@Stem + '_Initiatives');
    SET @tbl_epic  = QUOTENAME(@InputSchema) + '.' + QUOTENAME(@Stem + '_Epics');
    SET @tbl_iwork = QUOTENAME(@InputSchema) + '.' + QUOTENAME(@Stem + '_Initiatives_Work');
    SET @tbl_ework = QUOTENAME(@InputSchema) + '.' + QUOTENAME(@Stem + '_Epics_Work');

    -- Change count tracking
    IF OBJECT_ID('tempdb..#changes') IS NOT NULL DROP TABLE #changes;
    CREATE TABLE #changes (
        source  nvarchar(10),
        col     nvarchar(256),
        cnt     int
    );

    -- FY25 expired ESIs — excluded from active ESI check
    IF OBJECT_ID('tempdb..#fy25_esi') IS NOT NULL DROP TABLE #fy25_esi;
    CREATE TABLE #fy25_esi (esi_name nvarchar(256));
    INSERT INTO #fy25_esi VALUES
        ('Network Optimization (FY25 ESI)'),
        ('Digital Intelligence (FY25 ESI)'),
        ('Europe - Operations (FY25 ESI)'),
        ('Digital Experience (FY25 ESI)');

    -- Clone raw input into working copies — originals are never mutated
    SET @sql = N'IF OBJECT_ID(''' + REPLACE(@tbl_iwork,'''','''''') + N''') IS NOT NULL DROP TABLE ' + @tbl_iwork;
    EXEC sp_executesql @sql;
    SET @sql = N'IF OBJECT_ID(''' + REPLACE(@tbl_ework,'''','''''') + N''') IS NOT NULL DROP TABLE ' + @tbl_ework;
    EXEC sp_executesql @sql;

    SET @sql = N'SELECT * INTO ' + @tbl_iwork + N' FROM ' + @tbl_init;
    EXEC sp_executesql @sql;
    SET @sql = N'SELECT * INTO ' + @tbl_ework + N' FROM ' + @tbl_epic;
    EXEC sp_executesql @sql;

    -- ── Step 2a: Value Transformations ───────────────────────────────────────

    -- ── Mapping 1: Stage — Initiatives only ──────────────────────
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Stage')
    BEGIN
        -- Capture L0/SL1 rows before deleting (written to Deleted_Records tab)
        IF OBJECT_ID('tempdb..##deleted_init') IS NOT NULL DROP TABLE ##deleted_init;
        SET @sql = N'SELECT * INTO ##deleted_init FROM ' + @tbl_iwork + N'
                     WHERE LTRIM(RTRIM(ISNULL([Stage], ''''))) IN (''A: L0'', ''B: SL1'');';
        EXEC sp_executesql @sql;

        -- Delete L0 and SL1 rows
        SET @sql = N'DELETE FROM ' + @tbl_iwork + N'
                     WHERE LTRIM(RTRIM(ISNULL([Stage], ''''))) IN (''A: L0'', ''B: SL1'');';
        EXEC sp_executesql @sql;
        SET @removed_in = @@ROWCOUNT;

        -- Remap SL5/L5 → L4
        SET @sql = N'
            DECLARE @cnt_stage int;
            SELECT @cnt_stage = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Stage], ''''))) IN (''J: SL5'', ''K: L5'');
            UPDATE ' + @tbl_iwork + N'
            SET [Stage] = CASE LTRIM(RTRIM([Stage]))
                WHEN ''J: SL5'' THEN ''I: L4''
                WHEN ''K: L5''  THEN ''I: L4''
                ELSE [Stage]
            END
            WHERE LTRIM(RTRIM(ISNULL([Stage], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Stage (SL5/L5→L4)'', @cnt_stage);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 2: Work Status — Epics only ─────────────────────
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Work Status')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ws int;
            SELECT @cnt_ws = COUNT(*) FROM ' + @tbl_ework + N'
            WHERE LTRIM(RTRIM(ISNULL([Work Status], ''''))) <> ''''
              AND [Work Status] <> CASE LTRIM(RTRIM([Work Status]))
                    WHEN ''Not Started''       THEN ''New''
                    WHEN ''Approved''          THEN ''Active''
                    WHEN ''In Progress''       THEN ''Active''
                    WHEN ''On Hold''           THEN ''On Hold''
                    WHEN ''Closed''            THEN ''Completed/Closed''
                    WHEN ''Completed''         THEN ''Completed/Closed''
                    WHEN ''Assumed Completed'' THEN ''Completed/Closed''
                    WHEN ''Cancelled''         THEN ''Cancelled''
                    WHEN ''Rejected''          THEN ''Rejected''
                    ELSE [Work Status]
                END;
            UPDATE ' + @tbl_ework + N'
            SET [Work Status] = CASE LTRIM(RTRIM([Work Status]))
                WHEN ''Not Started''       THEN ''New''
                WHEN ''Approved''          THEN ''Active''
                WHEN ''In Progress''       THEN ''Active''
                WHEN ''On Hold''           THEN ''On Hold''
                WHEN ''Closed''            THEN ''Completed/Closed''
                WHEN ''Completed''         THEN ''Completed/Closed''
                WHEN ''Assumed Completed'' THEN ''Completed/Closed''
                WHEN ''Cancelled''         THEN ''Cancelled''
                WHEN ''Rejected''          THEN ''Rejected''
                ELSE [Work Status]
            END
            WHERE LTRIM(RTRIM(ISNULL([Work Status], ''''))) <> '''';
            INSERT INTO #changes VALUES (''epic'', ''Work Status (old→new)'', @cnt_ws);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 3: Estimated Annualized Value Range — both sheets ─
    DECLARE @evr_case nvarchar(MAX) = N'
        CASE LTRIM(RTRIM([Estimated Annualized Value Range]))
            WHEN ''1: Unknown''                     THEN ''''
            WHEN ''2: Low = < $1M''                 THEN ''1: Low = < $1M''
            WHEN ''3: Medium = $1M < Value < $10M'' THEN ''2: Medium = $1M < Value < $10M''
            WHEN ''4: High = > $10M''               THEN ''3: High = > $10M''
            ELSE [Estimated Annualized Value Range]
        END';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Estimated Annualized Value Range')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_evr_i int;
            SELECT @cnt_evr_i = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Estimated Annualized Value Range], ''''))) <> ''''
              AND [Estimated Annualized Value Range] <> ' + @evr_case + N';
            UPDATE ' + @tbl_iwork + N'
            SET [Estimated Annualized Value Range] = ' + @evr_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Estimated Annualized Value Range], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Estimated Value Range (renumbered)'', @cnt_evr_i);
        ';
        EXEC sp_executesql @sql;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Estimated Annualized Value Range')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_evr_e int;
            SELECT @cnt_evr_e = COUNT(*) FROM ' + @tbl_ework + N'
            WHERE LTRIM(RTRIM(ISNULL([Estimated Annualized Value Range], ''''))) <> ''''
              AND [Estimated Annualized Value Range] <> ' + @evr_case + N';
            UPDATE ' + @tbl_ework + N'
            SET [Estimated Annualized Value Range] = ' + @evr_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Estimated Annualized Value Range], ''''))) <> '''';
            INSERT INTO #changes VALUES (''epic'', ''Estimated Value Range (renumbered)'', @cnt_evr_e);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 4: Home Portfolio — both sheets ──────────────────
    DECLARE @hp_col nvarchar(256), @hp_case nvarchar(MAX);

    DECLARE @hp_init TABLE (col nvarchar(256));
    INSERT INTO @hp_init VALUES
        ('Demand Domain or Portfolio'), ('Portfolio'), ('Domain'), ('Home Portfolio');

    DECLARE hp_i CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @hp_init;
    OPEN hp_i; FETCH NEXT FROM hp_i INTO @hp_col;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = @hp_col)
        BEGIN
            SET @hp_case = N'CASE LTRIM(RTRIM(' + QUOTENAME(@hp_col) + N'))
                WHEN ''Data & AI'' THEN ''Platforms'' ELSE ' + QUOTENAME(@hp_col) + N' END';
            SET @sql = N'
                DECLARE @cnt_hp_i int;
                SELECT @cnt_hp_i = COUNT(*) FROM ' + @tbl_iwork + N'
                WHERE LTRIM(RTRIM(' + QUOTENAME(@hp_col) + N')) = ''Data & AI'';
                UPDATE ' + @tbl_iwork + N'
                SET ' + QUOTENAME(@hp_col) + N' = ' + @hp_case + N'
                WHERE LTRIM(RTRIM(ISNULL(' + QUOTENAME(@hp_col) + N', ''''))) <> '''';
                INSERT INTO #changes VALUES (''init'', ''Home Portfolio [' + @hp_col + N']'', @cnt_hp_i);
            ';
            EXEC sp_executesql @sql;
        END
        FETCH NEXT FROM hp_i INTO @hp_col;
    END
    CLOSE hp_i; DEALLOCATE hp_i;

    DECLARE @hp_epic TABLE (col nvarchar(256));
    INSERT INTO @hp_epic VALUES
        ('Home Domain/Portfolio'), ('Portfolio'), ('Domain'), ('Home Portfolio');

    DECLARE hp_e CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @hp_epic;
    OPEN hp_e; FETCH NEXT FROM hp_e INTO @hp_col;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = @hp_col)
        BEGIN
            SET @hp_case = N'CASE LTRIM(RTRIM(' + QUOTENAME(@hp_col) + N'))
                WHEN ''Data & AI'' THEN ''Platforms'' ELSE ' + QUOTENAME(@hp_col) + N' END';
            SET @sql = N'
                DECLARE @cnt_hp_e int;
                SELECT @cnt_hp_e = COUNT(*) FROM ' + @tbl_ework + N'
                WHERE LTRIM(RTRIM(' + QUOTENAME(@hp_col) + N')) = ''Data & AI'';
                UPDATE ' + @tbl_ework + N'
                SET ' + QUOTENAME(@hp_col) + N' = ' + @hp_case + N'
                WHERE LTRIM(RTRIM(ISNULL(' + QUOTENAME(@hp_col) + N', ''''))) <> '''';
                INSERT INTO #changes VALUES (''epic'', ''Home Portfolio [' + @hp_col + N']'', @cnt_hp_e);
            ';
            EXEC sp_executesql @sql;
        END
        FETCH NEXT FROM hp_e INTO @hp_col;
    END
    CLOSE hp_e; DEALLOCATE hp_e;

    -- ── Mapping 5: Demand SubType — Initiatives only ─────────────
    DECLARE @dst_col nvarchar(256);
    DECLARE @dst_cols TABLE (col nvarchar(256));
    INSERT INTO @dst_cols VALUES ('Demand SubType'), ('Demand_SubType');

    DECLARE dst CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @dst_cols;
    OPEN dst; FETCH NEXT FROM dst INTO @dst_col;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = @dst_col)
        BEGIN
            SET @sql = N'
                DECLARE @cnt_dst int;
                SELECT @cnt_dst = COUNT(*) FROM ' + @tbl_iwork + N'
                WHERE LTRIM(RTRIM(' + QUOTENAME(@dst_col) + N')) = ''Protect Purple'';
                UPDATE ' + @tbl_iwork + N'
                SET ' + QUOTENAME(@dst_col) + N' =
                    CASE LTRIM(RTRIM(' + QUOTENAME(@dst_col) + N'))
                        WHEN ''Protect Purple'' THEN ''Infosec (Protect Purple)''
                        ELSE ' + QUOTENAME(@dst_col) + N'
                    END
                WHERE LTRIM(RTRIM(ISNULL(' + QUOTENAME(@dst_col) + N', ''''))) <> '''';
                INSERT INTO #changes VALUES (''init'', ''Demand SubType (Protect Purple→Infosec)'', @cnt_dst);
            ';
            EXEC sp_executesql @sql;
        END
        FETCH NEXT FROM dst INTO @dst_col;
    END
    CLOSE dst; DEALLOCATE dst;

    -- ── Mapping 6: Milestone Type — Epics only ───────────────────
    DECLARE @mt_col nvarchar(256);
    DECLARE @mt_cols TABLE (col nvarchar(256));
    INSERT INTO @mt_cols VALUES
        ('Task or Milestone Type'), ('Milestone Type'), ('Milestone Type (Old)');

    DECLARE mt CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @mt_cols;
    OPEN mt; FETCH NEXT FROM mt INTO @mt_col;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = @mt_col)
        BEGIN
            SET @sql = N'
                DECLARE @cnt_mt int;
                SELECT @cnt_mt = COUNT(*) FROM ' + @tbl_ework + N'
                WHERE LTRIM(RTRIM(' + QUOTENAME(@mt_col) + N'))
                      IN (''Technology / Systems'',''Finance'',''Legal'',''Other dependency'');
                UPDATE ' + @tbl_ework + N'
                SET ' + QUOTENAME(@mt_col) + N' =
                    CASE LTRIM(RTRIM(' + QUOTENAME(@mt_col) + N'))
                        WHEN ''Technology / Systems'' THEN ''Technology''
                        WHEN ''Finance''              THEN ''Other''
                        WHEN ''Legal''                THEN ''Legal / Regulatory''
                        WHEN ''Other dependency''     THEN ''Other''
                        ELSE ' + QUOTENAME(@mt_col) + N'
                    END
                WHERE LTRIM(RTRIM(ISNULL(' + QUOTENAME(@mt_col) + N', ''''))) <> '''';
                INSERT INTO #changes VALUES (''epic'', ''Milestone Type [' + @mt_col + N']'', @cnt_mt);
            ';
            EXEC sp_executesql @sql;
        END
        FETCH NEXT FROM mt INTO @mt_col;
    END
    CLOSE mt; DEALLOCATE mt;

    -- ── Mapping 7: Demand Type — both sheets ───────────────────
    -- Maps old demand type values to new system values: Discretionary / Non Discretionary
    DECLARE @dt_case nvarchar(MAX) = N'
        CASE LTRIM(RTRIM([Demand Type]))
            WHEN ''Business w/ Tech''           THEN ''Discretionary''
            WHEN ''Business w/ Tech Initiative'' THEN ''Discretionary''
            WHEN ''Business Only''              THEN ''Non-Discretionary''
            WHEN ''Business Only Initiative''   THEN ''Non-Discretionary''
            WHEN ''Local Enhancement''          THEN ''Discretionary''
            WHEN ''Local Enhancement Epic''     THEN ''Discretionary''
            WHEN ''Lifecycle Management''       THEN ''Non-Discretionary''
            WHEN ''Lifecycle Management Epic''  THEN ''Non-Discretionary''
            ELSE [Demand Type]
        END';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Demand Type')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_dt_i int;
            SELECT @cnt_dt_i = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Type], ''''))) <> ''''
              AND [Demand Type] <> ' + @dt_case + N';
            UPDATE ' + @tbl_iwork + N'
            SET [Demand Type] = ' + @dt_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Type], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Demand Type (old→Discretionary/Non Discretionary)'', @cnt_dt_i);
        ';
        EXEC sp_executesql @sql;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Demand Type')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_dt_e int;
            SELECT @cnt_dt_e = COUNT(*) FROM ' + @tbl_ework + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Type], ''''))) <> ''''
              AND [Demand Type] <> ' + @dt_case + N';
            UPDATE ' + @tbl_ework + N'
            SET [Demand Type] = ' + @dt_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Type], ''''))) <> '''';
            INSERT INTO #changes VALUES (''epic'', ''Demand Type (old→Discretionary/Non Discretionary)'', @cnt_dt_e);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 8: Impacted Portfolios — both sheets ─────────
    -- Replace 'Data & AI' with 'Platforms' within comma-separated multi-value string
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Impacted Portfolios')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ip_i int;
            SELECT @cnt_ip_i = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE [Impacted Portfolios] LIKE ''%Data & AI%'';
            UPDATE ' + @tbl_iwork + N'
            SET [Impacted Portfolios] = REPLACE([Impacted Portfolios], ''Data & AI'', ''Platforms'')
            WHERE [Impacted Portfolios] LIKE ''%Data & AI%'';
            INSERT INTO #changes VALUES (''init'', ''Impacted Portfolios (Data & AI→Platforms)'', @cnt_ip_i);
        ';
        EXEC sp_executesql @sql;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Impacted Portfolios')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ip_e int;
            SELECT @cnt_ip_e = COUNT(*) FROM ' + @tbl_ework + N'
            WHERE [Impacted Portfolios] LIKE ''%Data & AI%'';
            UPDATE ' + @tbl_ework + N'
            SET [Impacted Portfolios] = REPLACE([Impacted Portfolios], ''Data & AI'', ''Platforms'')
            WHERE [Impacted Portfolios] LIKE ''%Data & AI%'';
            INSERT INTO #changes VALUES (''epic'', ''Impacted Portfolios (Data & AI→Platforms)'', @cnt_ip_e);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 9: Demand Domain or Portfolio — both sheets ────
    DECLARE @ddp_case nvarchar(MAX) = N'
        CASE LTRIM(RTRIM([Demand Domain or Portfolio]))
            WHEN ''APAC Domain''                              THEN ''APAC''
            WHEN ''Airline Domain''                           THEN ''Airline''
            WHEN ''Americas International''                   THEN ''Americas International''
            WHEN ''Commercial Domain''                        THEN ''Commercial''
            WHEN ''Commercial Portfolio''                     THEN ''''
            WHEN ''Data and Tech Domain''                     THEN ''Data and Tech''
            WHEN ''Data & AI Portfolio''                      THEN ''''
            WHEN ''Dock Domain''                              THEN ''Dock''
            WHEN ''Enterprise Services Portfolio''            THEN ''''
            WHEN ''Europe Domain''                            THEN ''Europe''
            WHEN ''Freight Domain''                           THEN ''Freight''
            WHEN ''Global Air Hubs & Ramps Domain''           THEN ''Global Air Hubs & Ramps''
            WHEN ''Global Capabilities Strategy Domain''      THEN ''Global Capabilities Strategy''
            WHEN ''Global Clearance Domain''                  THEN ''Global Clearance''
            WHEN ''Linehaul Domain''                          THEN ''Linehaul''
            WHEN ''MEISA Domain''                             THEN ''MEISA''
            WHEN ''Network 2.0 Domain''                       THEN ''Network 2.0''
            WHEN ''P&D Domain''                               THEN ''P&D''
            WHEN ''Platform Portfolio''                       THEN ''''
            WHEN ''Platforms Portfolio''                      THEN ''''
            WHEN ''Procurement Domain''                       THEN ''Procurement''
            WHEN ''Safety Domain''                            THEN ''Safety''
            WHEN ''Service Domain''                           THEN ''Service''
            WHEN ''SG&A Domain''                              THEN ''SG&A''
            WHEN ''Supply Chain Operations Portfolio''        THEN ''''
            WHEN ''Surface Fleet and Support Equipment Domain'' THEN ''Surface Fleet and Support Equipment''
            WHEN ''Surface Operations Domain''                THEN ''Surface Operations''
            WHEN ''Tricolor Domain''                          THEN ''Tricolor''
            ELSE [Demand Domain or Portfolio]
        END';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Demand Domain or Portfolio')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ddp_i int;
            SELECT @cnt_ddp_i = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Domain or Portfolio], ''''))) <> ''''
              AND [Demand Domain or Portfolio] <> ' + @ddp_case + N';
            UPDATE ' + @tbl_iwork + N'
            SET [Demand Domain or Portfolio] = ' + @ddp_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Domain or Portfolio], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Demand Domain or Portfolio (old→new)'', @cnt_ddp_i);
        ';
        EXEC sp_executesql @sql;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Demand Domain or Portfolio')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ddp_e int;
            SELECT @cnt_ddp_e = COUNT(*) FROM ' + @tbl_ework + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Domain or Portfolio], ''''))) <> ''''
              AND [Demand Domain or Portfolio] <> ' + @ddp_case + N';
            UPDATE ' + @tbl_ework + N'
            SET [Demand Domain or Portfolio] = ' + @ddp_case + N'
            WHERE LTRIM(RTRIM(ISNULL([Demand Domain or Portfolio], ''''))) <> '''';
            INSERT INTO #changes VALUES (''epic'', ''Demand Domain or Portfolio (old→new)'', @cnt_ddp_e);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 10: Is this confidential — Initiatives only ────
    -- Note: _sql_col() strips '?' so column name in SQL table is 'Is this confidential'
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Is this confidential')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_conf int;
            SELECT @cnt_conf = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Is this confidential], ''''))) = ''Confidential'';
            UPDATE ' + @tbl_iwork + N'
            SET [Is this confidential] = CASE LTRIM(RTRIM([Is this confidential]))
                WHEN ''Confidential'' THEN ''Yes - Privileged & Confidential''
                ELSE [Is this confidential]
            END
            WHERE LTRIM(RTRIM(ISNULL([Is this confidential], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Is this confidential (Confidential→Yes - Privileged & Confidential)'', @cnt_conf);
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Mapping 11: Lifecycle Status — Initiatives only ────────
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Lifecycle Status')
    BEGIN
        SET @sql = N'
            DECLARE @cnt_ls int;
            SELECT @cnt_ls = COUNT(*) FROM ' + @tbl_iwork + N'
            WHERE LTRIM(RTRIM(ISNULL([Lifecycle Status], ''''))) <> ''''
              AND [Lifecycle Status] <> CASE LTRIM(RTRIM([Lifecycle Status]))
                    WHEN ''Cancellation Request'' THEN ''Cancelled''
                    WHEN ''Completed''            THEN ''Completed/Closed''
                    ELSE [Lifecycle Status]
                END;
            UPDATE ' + @tbl_iwork + N'
            SET [Lifecycle Status] = CASE LTRIM(RTRIM([Lifecycle Status]))
                WHEN ''Cancellation Request'' THEN ''Cancelled''
                WHEN ''Completed''            THEN ''Completed/Closed''
                ELSE [Lifecycle Status]
            END
            WHERE LTRIM(RTRIM(ISNULL([Lifecycle Status], ''''))) <> '''';
            INSERT INTO #changes VALUES (''init'', ''Lifecycle Status (Cancellation Request→Cancelled, Completed→Completed/Closed)'', @cnt_ls);
        ';
        EXEC sp_executesql @sql;
    END

    DECLARE @removed_ep int = 0;

    -- ── Step 4a/4b: Classify Initiatives ─────────────────────────────────────
    SET @sql = N'
        ALTER TABLE ' + @tbl_iwork + N'
        ADD [Output_Segment]        nvarchar(200) NULL,
            [Future_State_Flow]     nvarchar(500) NULL,
            [Target_Lifecycle_Step] nvarchar(500) NULL,
            [Rule_ID_Applied]       nvarchar(100) NULL,
            [Migration_Source]      nvarchar(200) NULL;
    ';
    EXEC sp_executesql @sql;

    DECLARE @pc_expr nvarchar(500);
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Purple Chip')
        SET @pc_expr = N'LTRIM(RTRIM(ISNULL([Purple Chip], '''')))';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Does this request support a DRIVE strategic program?')
        SET @pc_expr = N'LTRIM(RTRIM(ISNULL([Does this request support a DRIVE strategic program?], '''')))';
    ELSE
        SET @pc_expr = N'''''';

    DECLARE @bc_expr nvarchar(500);
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Is this request vital to business continuity?')
        SET @bc_expr = N'LTRIM(RTRIM(ISNULL([Is this request vital to business continuity?], '''')))';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Is this non-discretionary demand vital to business continuity?')
        SET @bc_expr = N'LTRIM(RTRIM(ISNULL([Is this non-discretionary demand vital to business continuity?], '''')))';
    ELSE
        SET @bc_expr = N'''''';

    -- NRB expression
    DECLARE @nrb_expr nvarchar(500);
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = @NRB_Field)
        SET @nrb_expr = N'TRY_CAST(REPLACE(REPLACE(REPLACE('
                      + QUOTENAME(@NRB_Field)
                      + N', '','', ''''), ''$'', ''''), '' '', '''') AS float)';
    ELSE
        SET @nrb_expr = N'NULL';

    DECLARE @pending_case nvarchar(MAX) = N'
        CASE
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL2%'' THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL3%'' THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL4%'' THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL5%'' THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L0%''  THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L5%''  THEN 1
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],''''))))
                 NOT LIKE ''%L1%''
             AND UPPER(LTRIM(RTRIM(ISNULL([Stage],''''))))
                 NOT LIKE ''%L2%''
             AND UPPER(LTRIM(RTRIM(ISNULL([Stage],''''))))
                 NOT LIKE ''%L3%''
             AND UPPER(LTRIM(RTRIM(ISNULL([Stage],''''))))
                 NOT LIKE ''%L4%'' THEN 1
            ELSE 0
        END';

    DECLARE @lc_case nvarchar(MAX) = N'
        CASE
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL2%'' THEN N''PENDING — SL2 lifecycle step not defined''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL3%'' THEN N''PENDING — SL3 lifecycle step not defined''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL4%'' THEN N''PENDING — SL4 lifecycle step not defined''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL5%'' THEN N''PENDING — SL5 lifecycle step not defined''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L0%''  THEN N''PENDING — L0 not in migration scope''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L1%''  THEN N''Initial Request Information''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L2%''  THEN N''Architecture Alignment''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L3%''  THEN N''Demand Bundle Decomp and Conceptual Architecture''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L4%''  THEN N''Evaluate Outcome Achievement''
            WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L5%''  THEN N''PENDING — L5 lifecycle step not defined in rules file''
            ELSE N''PENDING — Stage not recognised''
        END';

    DECLARE @esi_active nvarchar(MAX) = N'(
        LTRIM(RTRIM(ISNULL([Enterprise Strategic Initiative ESI], '''')))
            NOT IN ('''', ''0-None'', ''nan'')
        AND NOT EXISTS (
            SELECT 1 FROM #fy25_esi fx
            WHERE fx.esi_name = LTRIM(RTRIM(ISNULL([Enterprise Strategic Initiative ESI], '''')))
        )
        AND ' + @pc_expr + N' NOT IN (''Purple Chip'', ''DRIVE Strategic Program'')
    )';

    SET @sql = N'
    UPDATE ' + @tbl_iwork + N'
    SET
        [Target_Lifecycle_Step] = ' + @lc_case + N',

        [Output_Segment] = CASE
            -- BR_TE_006: Non Discretionary + Business Only origin → Business Demand Management
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary''
             AND UPPER(' + @bc_expr + N') <> ''YES''
             AND ' + @pending_case + N' <> 1
             AND ' + @pc_expr + N' NOT IN (''Purple Chip'',''DRIVE Strategic Program'')
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) = ''''
                THEN ''TE-BusinessDemandMgmt''
            WHEN UPPER(' + @bc_expr + N') = ''YES''
                THEN ''TE-NonDisc-BusinessContinuity''
            WHEN ' + @pending_case + N' = 1
                THEN ''PENDING-Stage-Lifecycle-Undefined''
            WHEN ' + @pc_expr + N' IN (''Purple Chip'',''DRIVE Strategic Program'')
                THEN ''PC-StrategicProgram-PilotSandbox''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
                THEN CASE
                    WHEN ' + @nrb_expr + N' IS NULL                                    THEN ''TE-Disc-Other''
                    WHEN ' + @nrb_expr + N' < ' + CAST(@NRB_Threshold_M AS nvarchar) + N' THEN ''TE-Disc-StopWork-HOLD''
                    WHEN ' + @esi_active + N'                                           THEN ''TE-Disc-TransformationalInvestment''
                    ELSE ''TE-Disc-StopWork-HOLD''
                END
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
                THEN ''TE-Disc-Other''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary''
                THEN ''TE-Disc-Other''
            ELSE ''REVIEW-NoRuleMatched''
        END,

        [Future_State_Flow] = CASE
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary''
             AND UPPER(' + @bc_expr + N') <> ''YES''
             AND ' + @pending_case + N' <> 1
             AND ' + @pc_expr + N' NOT IN (''Purple Chip'',''DRIVE Strategic Program'')
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) = ''''
                THEN N''Business Demand Management''
            WHEN UPPER(' + @bc_expr + N') = ''YES''
                THEN N''Non-Discretionary – Business Continuity''
            WHEN ' + @pending_case + N' = 1
                THEN N''PENDING — '' + CASE
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL2%'' THEN N''SL2 lifecycle step not defined''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL3%'' THEN N''SL3 lifecycle step not defined''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL4%'' THEN N''SL4 lifecycle step not defined''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%SL5%'' THEN N''SL5 lifecycle step not defined''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L0%''  THEN N''L0 not in migration scope''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL([Stage],'''')))) LIKE ''%L5%''  THEN N''L5 lifecycle step not defined in rules file''
                    ELSE N''Stage not recognised''
                END
            WHEN ' + @pc_expr + N' IN (''Purple Chip'',''DRIVE Strategic Program'')
                THEN N''Strategic Program (Business + Tech) — Pilot Sandbox''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
                THEN CASE
                    WHEN ' + @nrb_expr + N' IS NULL                                    THEN N''Discretionary – Other''
                    WHEN ' + @nrb_expr + N' < ' + CAST(@NRB_Threshold_M AS nvarchar) + N' THEN N''STOP WORK Discretionary – Transformational Investment''
                    WHEN ' + @esi_active + N'                                           THEN N''Discretionary – Transformational Investment''
                    ELSE N''STOP WORK Discretionary – Transformational Investment''
                END
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
                THEN N''Discretionary – Other''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary''
                THEN N''Lifecycle Management — Carry as-is''
            ELSE N''REVIEW — No rule matched''
        END,

        [Rule_ID_Applied] = CASE
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary''
             AND UPPER(' + @bc_expr + N') <> ''YES''
             AND ' + @pending_case + N' <> 1
             AND ' + @pc_expr + N' NOT IN (''Purple Chip'',''DRIVE Strategic Program'')
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) = ''''             THEN ''BR_TE_006''
            WHEN UPPER(' + @bc_expr + N') = ''YES''                           THEN ''BR_TE_004''
            WHEN ' + @pending_case + N' = 1                                   THEN ''PENDING-Stage''
            WHEN ' + @pc_expr + N' IN (''Purple Chip'',''DRIVE Strategic Program'') THEN ''BR_PC_001''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
                THEN CASE
                    WHEN ' + @nrb_expr + N' IS NULL                                    THEN ''BR_TE_003 (NRB blank → Disc Other)''
                    WHEN ' + @nrb_expr + N' < ' + CAST(@NRB_Threshold_M AS nvarchar) + N' THEN ''BR_TE_002 (Stop Work)''
                    WHEN ' + @esi_active + N'                                           THEN ''BR_TE_001 (Strategic Investment)''
                    ELSE ''BR_TE_002 (L/XL no active ESI)''
                END
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''  THEN ''BR_TE_003 (≤M or blank)''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Non-Discretionary'' THEN ''LCM-Initiative''
            ELSE ''REVIEW''
        END,

        [Migration_Source] = CASE
            WHEN UPPER(' + @bc_expr + N') = ''YES''                           THEN ''FDXPROD → New Prod''
            WHEN ' + @pending_case + N' = 1                                   THEN ''PENDING''
            WHEN ' + @pc_expr + N' IN (''Purple Chip'',''DRIVE Strategic Program'') THEN ''FDXSANDBOXA → New Prod (ESPM owns)''
            WHEN LTRIM(RTRIM(ISNULL([Demand Type],''''))) = ''Discretionary''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
                THEN CASE
                    WHEN ' + @nrb_expr + N' IS NULL                                    THEN ''FDXPROD → New Prod''
                    WHEN ' + @nrb_expr + N' < ' + CAST(@NRB_Threshold_M AS nvarchar) + N' THEN ''FDXPROD → New Prod (HOLD)''
                    WHEN ' + @esi_active + N'                                           THEN ''FDXPROD → New Prod''
                    ELSE ''FDXPROD → New Prod (HOLD)''
                END
            ELSE ''FDXPROD → New Prod''
        END
    ';
    EXEC sp_executesql @sql;

    -- ── Step 4c: Classify Epics ──────────────────────────────────────────────
    SET @sql = N'
        ALTER TABLE ' + @tbl_ework + N'
        ADD [Output_Segment]        nvarchar(200) NULL,
            [Future_State_Flow]     nvarchar(500) NULL,
            [Target_Lifecycle_Step] nvarchar(500) NULL,
            [Rule_ID_Applied]       nvarchar(100) NULL,
            [Migration_Source]      nvarchar(200) NULL,
            [NewID_Temp]            nvarchar(50)  NULL,
            [Parent_Work_ID]        nvarchar(200) NULL,
            [Execution_Type]        nvarchar(200) NULL,
            [Kanban_Status]         nvarchar(100) NULL,
            [EPG_Approval]          nvarchar(100) NULL;
    ';
    EXEC sp_executesql @sql;

    DECLARE @parent_expr nvarchar(256);
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Associated Initiative')
        SET @parent_expr = N'LTRIM(RTRIM(ISNULL([Associated Initiative], '''')))';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Associated Initiative Seq ID')
        SET @parent_expr = N'LTRIM(RTRIM(ISNULL([Associated Initiative Seq ID], '''')))';
    ELSE
        SET @parent_expr = N'''''';

    DECLARE @vr_expr nvarchar(256);
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Estimated Annualized Value Range')
        SET @vr_expr = N'LTRIM(RTRIM(ISNULL([Estimated Annualized Value Range], '''')))';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_ework) AND name = 'Estimated Value Range')
        SET @vr_expr = N'LTRIM(RTRIM(ISNULL([Estimated Value Range], '''')))';
    ELSE
        SET @vr_expr = N'''''';

    DECLARE @is_milestone nvarchar(MAX) = N'(
        [Work Type] LIKE ''%Milestone%''
        OR [Work Type] LIKE ''%Risk%''
        OR [Work Type] = ''Initiative Milestones & Risks''
    )';

    -- NewID_Temp: sequential index for non-milestone rows in insertion order
    SET @sql = N'
        ;WITH ranked AS (
            SELECT
                [Run_ID], [Load_Timestamp],
                CASE WHEN ' + @is_milestone + N' THEN 1 ELSE 0 END AS is_ms,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE WHEN ' + @is_milestone + N' THEN 1 ELSE 0 END
                    ORDER BY (SELECT NULL)
                ) AS rn
            FROM ' + @tbl_ework + N'
        )
        UPDATE t
        SET
            [NewID_Temp]     = CASE WHEN r.is_ms = 1 THEN ''''
                               ELSE ''NewID-'' + RIGHT(''0000'' + CAST(r.rn AS nvarchar(10)), 4) END,
            [Parent_Work_ID] = ' + @parent_expr + N'
        FROM ' + @tbl_ework + N' t
        JOIN ranked r
          ON t.[Run_ID]         = r.[Run_ID]
         AND t.[Load_Timestamp] = r.[Load_Timestamp];
    ';
    EXEC sp_executesql @sql;

    SET @sql = N'
    UPDATE ' + @tbl_ework + N'
    SET
        [Output_Segment] = CASE
            WHEN ' + @is_milestone + N'                                                              THEN ''MILESTONE-RISK''
            WHEN LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%w/%''
              OR LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%biz w/%''                    THEN ''BWT-Epic-BelowPPL''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Lifecycle Management Epic''              THEN ''LCM-NonDisc-RunTheBusiness''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
             AND ' + @vr_expr + N' = ''2: Medium = $1M < Value < $10M''                             THEN ''LE-Disc-StopWork-HOLD''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')                  THEN ''LE-Disc-TransformationalInvestment''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''                 THEN ''LE-Disc-Other''
            ELSE ''REVIEW-NoRuleMatched''
        END,

        [Future_State_Flow] = CASE
            WHEN ' + @is_milestone + N'                                                              THEN N''Milestone/Risk — separate tab (not in Epic migration scope)''
            WHEN LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%w/%''
              OR LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%biz w/%''                    THEN N''Business w/Tech Epic — Below PPL Task''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Lifecycle Management Epic''              THEN N''Non-Discretionary - Run the Business''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
             AND ' + @vr_expr + N' = ''2: Medium = $1M < Value < $10M''                             THEN N''STOP WORK Discretionary – Transformational Investment''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')                  THEN N''Discretionary – Transformational Investment''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''                 THEN N''Discretionary – Other''
            ELSE N''REVIEW — Unknown work type''
        END,

        [Target_Lifecycle_Step] = CASE
            WHEN ' + @is_milestone + N' THEN ''N/A''
            ELSE ''Refer to parent Initiative lifecycle step''
        END,

        [Rule_ID_Applied] = CASE
            WHEN ' + @is_milestone + N'                                                              THEN ''MILESTONE-SEPARATE''
            WHEN LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%w/%''
              OR LOWER(LTRIM(RTRIM(ISNULL([Work Type],'''')))) LIKE ''%biz w/%''                    THEN ''BR_TE_BWTE_001–004''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Lifecycle Management Epic''              THEN ''BR_TE_LCM_001''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
             AND ' + @vr_expr + N' = ''2: Medium = $1M < Value < $10M''                             THEN ''BR_TE_LE_003 (value range proxy)''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')                  THEN ''BR_TE_LE_001''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''1: XS'',''2: S'',''3: M'')         THEN ''BR_TE_LE_005''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''                 THEN ''BR_BLANK_001''
            ELSE ''REVIEW''
        END,

        [Migration_Source] = CASE
            WHEN ' + @is_milestone + N'                                                              THEN ''See Epics_Milestone_Risk tab''
            WHEN LTRIM(RTRIM(ISNULL([Work Type],''''))) = ''Local Enhancement Epic''
             AND LTRIM(RTRIM(ISNULL([T-Shirt Size],''''))) IN (''4: L'',''5: XL'')
             AND ' + @vr_expr + N' = ''2: Medium = $1M < Value < $10M''                             THEN ''FDXPROD → New Prod (HOLD)''
            ELSE ''FDXPROD → New Prod''
        END,

        [Execution_Type] = CASE WHEN ' + @is_milestone + N' THEN '''' ELSE ''Demand Bundle Epic at PPL+2'' END,
        [Kanban_Status]  = CASE WHEN ' + @is_milestone + N' THEN '''' ELSE ''Intake/New'' END,
        [EPG_Approval]   = ''''
    ';
    EXEC sp_executesql @sql;

    -- ── BWT Epics: inherit parent Initiative Future_State_Flow ────
    -- BR_TE_BWTE_001 to 004: BWT Epics inherit parent initiative flow.
    -- Join Epic.Parent_Work_ID to Initiative work ID column to get classified flow.
    -- Try both common Initiative ID column names.
    DECLARE @init_id_col nvarchar(256) = NULL;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Seq ID')
        SET @init_id_col = 'Seq ID';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'Work ID')
        SET @init_id_col = 'Work ID';
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@tbl_iwork) AND name = 'ID')
        SET @init_id_col = 'ID';

    IF @init_id_col IS NOT NULL
    BEGIN
        SET @sql = N'
            UPDATE e
            SET
                e.[Future_State_Flow] = ISNULL(i.[Future_State_Flow], e.[Future_State_Flow]),
                e.[Output_Segment]    = ISNULL(
                    CASE i.[Output_Segment]
                        WHEN ''TE-NonDisc-BusinessContinuity''      THEN ''BWT-Epic-BC''
                        WHEN ''TE-Disc-TransformationalInvestment''  THEN ''BWT-Epic-TransInv''
                        WHEN ''TE-Disc-StopWork-HOLD''               THEN ''BWT-Epic-StopWork''
                        WHEN ''TE-Disc-Other''                       THEN ''BWT-Epic-DiscOther''
                        WHEN ''TE-BusinessDemandMgmt''               THEN ''BWT-Epic-BizDemand''
                        ELSE NULL
                    END,
                    e.[Output_Segment]
                ),
                e.[Rule_ID_Applied]   = CASE
                    WHEN i.[Output_Segment] IS NOT NULL
                        THEN ''BR_TE_BWTE (inherits '' + ISNULL(i.[Rule_ID_Applied],'''') + '')''
                    ELSE e.[Rule_ID_Applied]
                END
            FROM ' + @tbl_ework + N' e
            JOIN ' + @tbl_iwork + N' i
              ON LTRIM(RTRIM(ISNULL(e.[Parent_Work_ID],''''))) = LTRIM(RTRIM(ISNULL(i.' + QUOTENAME(@init_id_col) + N','''')))
             AND LTRIM(RTRIM(ISNULL(e.[Parent_Work_ID],''''))) <> ''''
            WHERE (
                LOWER(LTRIM(RTRIM(ISNULL(e.[Work Type],''''))))  LIKE ''%w/%''
             OR LOWER(LTRIM(RTRIM(ISNULL(e.[Work Type],''''))))  LIKE ''%biz w/%''
            );
        ';
        EXEC sp_executesql @sql;
    END

    -- ── Return result sets to Python ─────────────────────────────────────────

    -- RS1: Classified Initiatives
    SET @sql = N'SELECT * FROM ' + @tbl_iwork;
    EXEC sp_executesql @sql;

    -- RS2: Classified Epics
    SET @sql = N'SELECT * FROM ' + @tbl_ework;
    EXEC sp_executesql @sql;

    -- RS3: Scalar counts
    DECLARE @new_id_count int = 0;
    SET @sql = N'SELECT @c = COUNT(*) FROM ' + @tbl_ework + N' WHERE [NewID_Temp] <> ''''';
    EXEC sp_executesql @sql, N'@c int OUTPUT', @c = @new_id_count OUTPUT;

    SELECT @removed_in AS removed_in, @removed_ep AS removed_ep, @new_id_count AS new_id_count;

    -- RS4: Value mapping counts — Initiatives
    SELECT col, cnt FROM #changes WHERE source = 'init' AND cnt > 0 ORDER BY col;

    -- RS5: Value mapping counts — Epics
    SELECT col, cnt FROM #changes WHERE source = 'epic' AND cnt > 0 ORDER BY col;

    -- RS6: Deleted Initiative rows (Stage A: L0 or B: SL1)
    IF OBJECT_ID('tempdb..##deleted_init') IS NOT NULL
        SELECT * FROM ##deleted_init;

    -- ── Cleanup working copies ───────────────────────────────────────────────
    SET @sql = N'IF OBJECT_ID(''' + REPLACE(@tbl_iwork,'''','''''') + N''') IS NOT NULL DROP TABLE ' + @tbl_iwork;
    EXEC sp_executesql @sql;
    SET @sql = N'IF OBJECT_ID(''' + REPLACE(@tbl_ework,'''','''''') + N''') IS NOT NULL DROP TABLE ' + @tbl_ework;
    EXEC sp_executesql @sql;

END
GO
