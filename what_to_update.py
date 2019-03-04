"""
Rules for writing the SQL code:
 1. Columns name need to match the names from Oracle tables
 2. a) and they need to come after the word `AS` (in capitals) in the FROM clause
    b) if nested queries are used, then use 'as' (in lowercase) in the other clauses (otherwise it will try to split on "AS" and fail)

 3. if [column_name] has NULL in it, use: ISNULL([column_name], '') AS ORACLE_COLUMN_NAME
 4. Each sql_statement needs to have a column called HASH_VALUE (otherwise the code fails)
 5. The word "DISTINCT" needs to be in capitals
 6. The syntax we HAVE TO use is in the 'sql_statement' is: "SELECT column1 AS column1, column2 AS column2 FROM ...". We cannot simply write: "SELECT column 1, column2 FROM ..."
"""

tables_to_update = [
                    # 3 Fizzy tables
                   #  {'oracle_table': 'SA_UNITS',
                   #   'company': 'TVH',
                   #   'hierarchy': 12,  # 'hierarchy' -> denotes the order in which SA_to_GD jobs are run
                   #   'server': 'TVHA-UH-DB03',
                   #   'on_prem_database': 'Fizzy',
                   #   'col_to_increment': 'B_LOADID',
                   #   'sql_statement': """SELECT 'Units' AS B_CLASSNAME,
                   #                              b.buildingRef_ AS F_BLOCKS ,
                   #
                   #                              u.[Property Code] AS UNITS_ID,
                   #                              u.[Property Name] AS PROPERTY_NAME,
                   #
                   #                              u.[Full Address] AS SHORT_ADDRESS,
                   #                              u.[Property Number] AS GEOADDRESS1, u.[Address Line1] AS GEOADDRESS2, u.[Address Line2] AS GEOADDRESS3,
                   #                              u.Street AS GEOTOWN, u.Postcode AS GEOPOSTCODE,
                   #
                   #                              ISNULL(u.Bedrooms, '') AS NO_OF_BEDROOMS,
                   #
                   #                              case
                   #                              when u.[Property Status] like 'Available' then '022'
                   #                              when u.[Property Status] like 'Booked' then '022'
                   #                              when u.[Property Status] like 'Occupied' then '004'
                   #                              when u.[Property Status] like 'On Hold' then '024'
                   #                              else '000' end AS OCCUPANCY_STATUS,
                   #
                   #
                   #                              case when u.Floor = '1' then '004' when u.Floor = '2' then '005' when u.Floor = '3' then '006' when u.Floor = '4' then '007' when u.Floor = '5' then '009'  when u.Floor = '6' then '010' when u.Floor = '7' then '011' when u.Floor = '8' then '012' when u.Floor = '9' then '013' when u.Floor = '10' then '014' when u.Floor = '11' then '015' when u.Floor = '12' then '016' when u.Floor = '13' then '017' when u.Floor = '14' then '018' when u.Floor = '2/3-Mez' then '030' when u.Floor = 'G' then '002' when u.Floor = 'Ground' then '002' else '000' end AS FLOOR_LEVEL,
                   #
                   #                              u.Comments AS ANY_OTHER_INFORMATION,
                   #                              '3' AS F_DATA_OWNERSHIP,
                   #                              '3' AS F_SOURCE_SYSTEM,
                   #
                   #                              hashbytes('SHA1', ISNULL(b.buildingRef_,'NA') + ISNULL(u.[Property Code], 'NA') + ISNULL(u.[Property Name], 'NA') + ISNULL(u.[Full Address], 'NA') + ISNULL(u.[Property Number], 'NA') + ISNULL(u.[Address Line1], 'NA') + ISNULL(u.[Address Line2], 'NA') + ISNULL(u.Street, 'NA') + ISNULL(u.Postcode, 'NA') + ISNULL(u.Comments, 'NA')) AS HASH_VALUE
                   #
                   #                      FROM pexPropertyIndex u
                   #                          inner join Building b on u.Building =b.name_
                   #                          inner join Area a on b.area_ = a.ID""",
                   #   'primary_key': 'UNITS_ID',
                   #   'delete_last': False
                   #  },
                   #
                   #   {'oracle_table': 'SA_BLOCKS',
                   #    'company': 'TVH',
                   #    'hierarchy': 11,
                   #   'server': 'TVHA-UH-DB03',
                   #   'on_prem_database': 'Fizzy',
                   #   'col_to_increment': 'B_LOADID',
                   #   'sql_statement': """SELECT 'Blocks' AS B_CLASSNAME,
                   #                             b.buildingRef_ AS BLOCKS_ID,
                   #                             b.name_ AS BLOCK_NAME,
                   #                             a.code_ AS F_SCHEMES,
                   #                             ad.line1_ AS GEOADDRESS1,
                   #                             ad.line2_ AS GEOADDRESS2,
                   #                             ad.line3_ AS GEOTOWN,
                   #                             ad.postcode_ AS GEOPOSTCODE,
                   #                             '3' AS F_SOURCE_SYSTEM,
                   #                             '3' AS F_DATA_OWNERSHIP
                   #
                   #                             ,CHECKSUM(b.buildingRef_, b.name_, a.code_, ad.line1_, ad.line2_, ad.line3_, ad.postcode_) AS HASH_VALUE
                   #                      FROM Building b
                   #                      inner join Area a on b.area_ = a.ID
                   #                      left join Address ad on b.address_ = ad.ID""",
                   #   'primary_key': 'BLOCKS_ID',
                   #   'delete_last': False
                   #  },
                   #
                   #   {'oracle_table': 'SA_SCHEMES',
                   #    'company': 'TVH',
                   #    'hierarchy': 9,
                   #   'server': 'TVHA-UH-DB03',
                   #   'on_prem_database': 'Fizzy',
                   #   'col_to_increment': 'B_LOADID',
                   #   'sql_statement': """SELECT 'Schemes' AS B_CLASSNAME,
                   #                               a.code_ AS SCHEMES_ID,
                   #                               a.name_ AS SCHEME_NAME,
                   #                               'FIZ_EST_001' AS F_ESTATES,
                   #                               '3' AS F_SOURCE_SYSTEM,
                   #                               '3' AS F_DATA_OWNERSHIP,
                   #
                   #                               CHECKSUM(a.code_, a.name_) AS HASH_VALUE
                   #                        FROM  Area a""",
                   #   'primary_key': 'SCHEMES_ID',
                   #   'delete_last': False
                   #   },
                   #
                   # # 1 SA_ESTATE_INSP_AND_CLEANING table # changed 05-Sep-2018
                   # {'oracle_table': 'SA_ESTATE_INSP_AND_CLEANING',
                   #  'company': 'TVH',
                   #  'hierarchy': 10,
                   #  'server': 'tvha-uh-ssrs',
                   #  'on_prem_database': 'MyTVH',
                   #  'col_to_increment': 'B_LOADID',
                   #  'sql_statement': """SELECT row_number() OVER (ORDER BY [prop_ref]) AS ESTATE_INSP_AND_CLEANING_
                   #                          ,ISNULL( rtrim([prop_ref]), '') AS FD_SCHEMES
                   #                          ,ISNULL( rtrim([resident_inspector]), '')  AS RESIDENT_INSPECTOR
                   #                          ,ISNULL( rtrim([inspection_priority]), '') AS PRIORITY
                   #                          ,ISNULL( rtrim(iif ([grounds_contractor] like 'Just Ask', 1, 21)), '') AS FD_CONTRACTOR_INSP
                   #                          ,ISNULL( rtrim(iif([grounds_team] like '',null,[grounds_team])), '') AS GROUNDS_TEAM
                   #                          ,ISNULL( rtrim(iif ([cleaning_contractor] like 'Cleanscapes', 4, 21)), '') AS FD_CONTRACTOR_CLEANING
                   #                          ,ISNULL( rtrim(iif([cleaning_team] = '', null ,[cleaning_team])), '') AS CLEANING_TEAM
                   #                          ,ISNULL( rtrim([staff_grounds_cnt]), '') AS STAFF_GROUNDS_COUNT
                   #                          ,ISNULL( convert(Date,rtrim([staff_grounds_last_date])), '') AS STAFF_GROUNDS_LASTDATE
                   #                          ,ISNULL( rtrim([staff_cleaning_cnt]), '') AS STAFF_CLEANING_COUNT
                   #                          ,ISNULL( convert(Date,rtrim([staff_cleaning_last_date])), '') AS LAST_CLEANING_DATE
                   #                          ,ISNULL( rtrim([grounds_status]), '') AS GROUND_STATUS
                   #                          ,ISNULL( convert(Date,rtrim([grounds_due])), '') AS GROUNDS_DUE
                   #                          ,ISNULL( rtrim([cleaning_status]), '') AS CLEANING_STATUS
                   #                          ,ISNULL( convert(Date,rtrim([cleaning_due])), '') AS CLEANING_DUE
                   #                          ,ISNULL( rtrim([resident_grounds_cnt]), '') AS RESIDENTS_COUNT
                   #                          ,ISNULL( convert(Date,rtrim([resident_grounds_last_date])), '') AS RESIDENT_GROUNDS_LAST_DAT
                   #                          ,ISNULL( rtrim([resident_cleaning_cnt]), '') AS RESIDENTS_CLEANING_COUNT
                   #                          ,ISNULL( convert(Date,rtrim([resident_cleaning_last_date])), '') AS RESIDENT_CLEAN_LAST_DATE
                   #
                   #                          ,'EstateInspAndCleaning' AS B_CLASSNAME
                   #                          ,checksum([prop_ref],[resident_inspector],[inspection_priority],[grounds_contractor],[grounds_team],[cleaning_contractor],[cleaning_team],[staff_grounds_cnt],[staff_grounds_last_date],[staff_cleaning_cnt],[staff_cleaning_last_date],[grounds_status],[grounds_due],[cleaning_status],[cleaning_due],[resident_grounds_cnt] ,[resident_grounds_last_date],[resident_cleaning_cnt],[resident_cleaning_last_date]) AS HASH_VALUE
                   #                          ,GETDATE() AS B_CREDATE
                   #                          ,'UH_SSRS_MyTvh Integration' AS B_CREATOR
                   #                    FROM u_vw_clearview_feedback_estate_inspections_scheme""",
                   #  'primary_key': 'ESTATE_INSP_AND_CLEANING_',
                   #  'delete_last': False
                   # },

                # 3 KEYSTONE tables
                {'oracle_table': 'SA_ATTRIBUTE_KEY_LOOKUP',  # changed 05-Sep-2017
                 'company': 'TVH',
                 'hierarchy': 5,
                 'server': 'TVHA-SQL4',
                 'on_prem_database': 'keystone_live',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': """SELECT DISTINCT b.ComponentID AS ATTRIBUTE_KEY_LOOKUP_ID,
                                                                  b.Component AS KEY_NAME,
                                                                  CHECKSUM(b.ComponentID,b.Component) AS HASH_VALUE,
                                                                  'AttributeKeyLookup' AS B_CLASSNAME,
                                                                  41 AS F_SOURCE_SYSTEM,
                                                                  1 AS F_DATA_OWNERSHIP,
                                                                  GETDATE() AS B_CREDATE,
                                                                  'Keystone Integration' AS B_CREATOR,
                                                                  dateadd(day,-1,getdate()) AS VALID_FROM,
                                                                  dateadd(month,100,getdate()) AS VALID_TO
                                                  FROM (
                                                                    Select distinct c.ComponentID,c.Component from (
                                                                                               SELECT
                                                                                               a.AssetID
                                                                                               ,a.UPRN
                                                                                               ,sa.RepairElementID
                                                                                               ,sa.RepairElement
                                                                                               ,sa.ComponentID
                                                                                               ,sa.Component
                                                                                               ,sa.AttributeID
                                                                                               ,sa.SurveyAttributeID
                                                                                               ,sa.Attribute
                                                                                               ,sa.InstallationDate
                                                                                               ,NULL as RepairID
                                                                                               ,NULL as SurveyRepairID
                                                                                               ,NULL as RepairDescription
                                                                                               ,NULL as Quantity
                                                                                               ,NULL as YearDue
                                                                                               ,NULL as RepairCost
                                                                                               ,sa.Surveyor
                                                                                               FROM MIView_Asset a
                                                                                               JOIN MIView_Asset_SurveyAttributes sa ON a.AssetID = sa.AssetID
                                                                                               UNION ALL
                                                                                               SELECT
                                                                                               a.AssetID
                                                                                               ,a.UPRN
                                                                                               ,NULL
                                                                                               ,sr.RepairElement
                                                                                               ,sr.ComponentID
                                                                                               ,sr.Component
                                                                                               ,NULL
                                                                                               ,NULL
                                                                                               ,NULL
                                                                                               ,NULL
                                                                                               ,sr.RepairID
                                                                                               ,sr.SurveyRepairID
                                                                                               ,sr.RepairDescription
                                                                                               ,sr.Quantity
                                                                                               ,sr.YearDue
                                                                                               ,sr.RepairCost
                                                                                               ,sr.Surveyor
                                                                                               FROM MIView_Asset a
                                                                                               JOIN MIView_Asset_SurveyRepairs sr ON a.AssetID = sr.AssetID
                                                                                               ) c
                                                                           ) b
                                              """,
                 'primary_key': 'ATTRIBUTE_KEY_LOOKUP_ID',
                 'delete_last': False
                 },

                {'oracle_table': 'SA_PROP_ATTRIBUTE_KEY_VALUE',  # 284,274 rows # changed 05-Sep-2018
                 'company': 'TVH',
                 'hierarchy': 14,
                 'server': 'TVHA-SQL4',
                 'on_prem_database': 'keystone_live',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': """ SELECT ROW_NUMBER() over (order by c.UPRN, c.SurveyAttributeID) AS PROP_ATTRIBUTE_KEY_VALUE_
                                                        ,c.UPRN AS F_PROPERTY_ATTRIBUTES ,
                                                        c.Attribute AS ATTRIBUTE_VALUE,
                                                        c.ComponentID AS F_ATTRIBUTE_KEY_LOOKUP,
                                                        c.InstallationDate AS ATTRIBUTE_DATE,
                                                        c.Quantity AS QUANTITY ,
                                                        'TP_' + Convert (Varchar,c.RepairElementID) AS F_PROP_ATTRIBUTE_TYPE ,
                                                        c.Replacement AS REPLACEMENT ,
                                                        c.ReplacementCost AS REPLACEMENT_COST ,
                                                        c.Surveyor AS SURVEYOR ,
                                                        c.YearDue AS  DUE_DATE,
                                                        CHECKSUM(c.SurveyAttributeID,c.UPRN,c.Attribute,c.ComponentID,c.InstallationDate,c.Quantity,c.RepairElementID,c.Replacement,c.ReplacementCost,c.Surveyor,c.YearDue) AS HASH_VALUE,
                                                        'PropAttributeKeyValue' AS B_CLASSNAME,
                                                                            '41' AS F_SOURCE_SYSTEM,
                                                                    '1' AS F_DATA_OWNERSHIP,
                                                                            GETDATE() AS B_CREDATE,
                                                                            'Keystone Integration' AS B_CREATOR
                                                                            FROM (
                                                        SELECT AssetID,UPRN,max(RepairElementID) AS RepairElementID ,RepairElement,ComponentID,Component
                                                        ,Max(Attribute) AS Attribute, Max(InstallationDate) InstallationDate,MAX(RepairDescription) AS Replacement,Max(RepairCost)as ReplacementCost,
                                                        Max(Quantity) AS Quantity ,Max(YearDue) AS YearDue,max(isnull(SurveyAttributeID,SurveyRepairID)) AS SurveyAttributeID
                                                        ,Max(Surveyor) Surveyor

                                                        FROM (
                                                                                   SELECT * FROM (
                                                                                   SELECT
                                                                                   a.AssetID
                                                                                   ,a.UPRN
                                                                                   ,sa.RepairElementID
                                                                                   ,sa.RepairElement
                                                                                   ,sa.ComponentID
                                                                                   ,sa.Component
                                                                                   ,sa.AttributeID
                                                                                   ,sa.SurveyAttributeID
                                                                                   ,sa.Attribute
                                                                                   ,sa.InstallationDate
                                                                                   ,NULL AS RepairID
                                                                                   ,NULL AS SurveyRepairID
                                                                                   ,NULL AS RepairDescription
                                                                                   ,NULL AS Quantity
                                                                                   ,NULL AS YearDue
                                                                                   ,NULL AS RepairCost
                                                                                   ,sa.Surveyor
                                                                                   FROM MIView_Asset a
                                                                                   JOIN MIView_Asset_SurveyAttributes sa ON a.AssetID = sa.AssetID
                                                                                   UNION ALL
                                                                                   SELECT
                                                                                   a.AssetID
                                                                                   ,a.UPRN
                                                                                   ,NULL
                                                                                   ,sr.RepairElement
                                                                                   ,sr.ComponentID
                                                                                   ,sr.Component
                                                                                   ,NULL
                                                                                   ,NULL
                                                                                   ,NULL
                                                                                   ,NULL
                                                                                   ,sr.RepairID
                                                                                   ,sr.SurveyRepairID
                                                                                   ,sr.RepairDescription
                                                                                   ,sr.Quantity
                                                                                   ,sr.YearDue
                                                                                   ,sr.RepairCost
                                                                                   ,sr.Surveyor
                                                                                   FROM MIView_Asset a
                                                                                   JOIN MIView_Asset_SurveyRepairs sr ON a.AssetID = sr.AssetID
                                                                                   ) a
                                                        ) b where RepairElement not in ('HHSRS','Portfolio Investment Opportunity ' )
                                                        group by b.ComponentID,b.AssetID,b.UPRN,b.RepairElement,b.Component
                                                        ) c
                                                  """,
                 'primary_key': 'PROP_ATTRIBUTE_KEY_VALUE_',
                 'delete_last': False
                 },

                {'oracle_table': 'SA_PROP_ATTRIBUTE_TYPE',   # changed 05-Sep-2018
                 'company': 'TVH',
                 'hierarchy': 4,
                 'server': 'TVHA-SQL4',
                 'on_prem_database': 'Keystone_Live',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': """SELECT DISTINCT 'TP_' + cast(b.RepairElementID as Varchar) AS PROP_ATTRIBUTE_TYPE_ID,
                                                                      b.RepairElement AS TYPE_NAME,
                                                                      'COMPONENTS' AS TYPE_CATEGORY,
                                                                      'Units' AS PROPERTY_LEVEL,
                                                                      CHECKSUM(b.RepairElementID,b.RepairElement) AS HASH_VALUE,
                                                                      'PropAttributeType' AS B_CLASSNAME,
                                                                        '41' AS F_SOURCE_SYSTEM,
                                                                        '1' AS F_DATA_OWNERSHIP,
                                                                        GETDATE() AS B_CREDATE,
                                                                        'Keystone Integration' AS B_CREATOR,
                                                                        dateadd(day,-1,getdate()) AS VALID_FROM,
                                                                        dateadd(month,100,getdate()) AS VALID_TO
                                                    FROM (

                                                        Select * from (
                                                                        SELECT
                                                                        a.AssetID
                                                                        ,a.UPRN
                                                                        ,sa.RepairElementID
                                                                        ,sa.RepairElement
                                                                        ,sa.ComponentID
                                                                        ,sa.Component
                                                                        ,sa.AttributeID
                                                                        ,sa.SurveyAttributeID
                                                                        ,sa.Attribute
                                                                        ,sa.InstallationDate
                                                                        ,NULL as RepairID
                                                                        ,NULL as SurveyRepairID
                                                                        ,NULL as RepairDescription
                                                                        ,NULL as Quantity
                                                                        ,NULL as YearDue
                                                                        ,NULL as RepairCost
                                                                        ,sa.Surveyor
                                                                        FROM MIView_Asset a
                                                                        JOIN MIView_Asset_SurveyAttributes sa ON a.AssetID = sa.AssetID
                                                                        UNION ALL
                                                                        SELECT
                                                                        a.AssetID
                                                                        ,a.UPRN
                                                                        ,NULL
                                                                        ,sr.RepairElement
                                                                        ,sr.ComponentID
                                                                        ,sr.Component
                                                                        ,NULL
                                                                        ,NULL
                                                                        ,NULL
                                                                        ,NULL
                                                                        ,sr.RepairID
                                                                        ,sr.SurveyRepairID
                                                                        ,sr.RepairDescription
                                                                        ,sr.Quantity
                                                                        ,sr.YearDue
                                                                        ,sr.RepairCost
                                                                        ,sr.Surveyor
                                                                        FROM MIView_Asset a
                                                                        JOIN MIView_Asset_SurveyRepairs sr ON a.AssetID = sr.AssetID
                                                                        ) a
                                                            ) b where RepairElementID is not null

                                                  """,
                 'primary_key': 'PROP_ATTRIBUTE_TYPE_ID',
                 'delete_last': False
                 },


                 # 11  VIEWS
                 {'oracle_table': 'SA_SCHEMES',
                   'company': 'TVH',
                  'hierarchy': 9 ,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_scheme',
                 'primary_key': 'SCHEMES_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_BLOCKS',
                  'company': 'TVH',
                  'hierarchy': 11,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_block',
                 'primary_key': 'BLOCKS_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_UNITS',
                  'company': 'TVH',
                  'hierarchy': 12,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_unit',  # 19,544 rows
                 'primary_key': 'UNITS_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_PROPERTY_TYPE',
                  'company': 'TVH',
                  'hierarchy': 3,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_propertytype',
                 'primary_key': 'PROPERTY_TYPE_ID',
                 'delete_last': False
                 },

                 # {'oracle_table': 'SA_MANAGING_AGENTS',  # Amy doesn't want this anymore
                 # 'server': 'TVHA-UH-DB01',
                 # 'on_prem_database': 'uhlive',
                 # 'col_to_increment': 'B_LOADID',
                 # 'sql_statement': 'SELECT * FROM semarchy_managingagent',
                 # 'primary_key': 'MANAGING_AGENTS_ID',
                 # 'delete_last': False
                 # },

                 {'oracle_table': 'SA_LOOKUP',
                  'company': 'TVH',
                  'hierarchy': 1,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_lookup',
                 'primary_key': 'LOOKUP_ID',
                 'delete_last': False
                 },

                {'oracle_table': 'SA_LOCAL_AUTHORITY',
                 'company': 'TVH',
                 'hierarchy': 6,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_localauthority',
                 'primary_key': 'LOCAL_AUTHORITY_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_TENURE_TYPE',
                  'company': 'TVH',
                  'hierarchy': 2,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_tenuretype',
                 'primary_key': 'TENURE_TYPE_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_STAFF_INVOLVEMENT',
                  'company': 'TVH',
                  'hierarchy': 8,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_staffinvolvement',
                 'primary_key': 'STAFF_INVOLVEMENT_ID',
                 'delete_last': False
                 },

                 {'oracle_table': 'SA_UNIT_CLUSTER',
                  'company': 'TVH',
                  'hierarchy': 7,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_cluster',
                 'primary_key': 'UNIT_CLUSTER_ID',
                 'delete_last': False
                 },

                {'oracle_table': 'SA_PROPERTY_ATTRIBUTES',
                 'company': 'TVH',
                 'hierarchy': 13,
                 'server': 'TVHA-UH-DB01',
                 'on_prem_database': 'uhlive',
                 'col_to_increment': 'B_LOADID',
                 'sql_statement': 'SELECT * FROM semarchy_property_attrib',
                 'primary_key': 'PROPERTY_ATTRIBUTES_ID',
                 'delete_last': False
                 },


                 # 8 New Views (Customer Data Model) - 18-Sep-2018
                 {'oracle_table': 'SA_RESIDENTS',  # 15,636
                  'company': 'TVH',
                  'hierarchy': 17,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_residents',
                  'primary_key': 'RESIDENTS_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_COMMUNICATION',  # 90,356
                  'company': 'TVH',
                  'hierarchy': 19,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_communication',
                  'primary_key': 'COMMUNICATION_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_VULNERABILTY_DETAILS',  # 28,780
                  'company': 'TVH',
                  'hierarchy': 19,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_vulnerabiltyDetails',
                  'primary_key': 'VULNERABILTY_DETAILS_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_ECONOMIC_STATUS',  # 28,780
                  'company': 'TVH',
                  'hierarchy': 19,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_economicStatus',
                  'primary_key': 'ECONOMIC_STATUS_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_CONTACT_PREFRENCES',  # spelling fucking mistake # 28,780
                  'company': 'TVH',
                  'hierarchy': 19,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_contactPreferences',
                  'primary_key': 'CONTACT_PREFRENCES_ID',  # spelling fucking mistake
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_RENT_GRP_REF',  # 9
                  'company': 'TVH',
                  'hierarchy': 15,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_rentGrp',
                  'primary_key': 'RENT_GRP_REF_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_PERSON',  # 28,
                  'company': 'TVH',
                  'hierarchy': 18,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_person',
                  'primary_key': 'PERSON_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_PERSON_LOOKUP',  # 278
                  'company': 'TVH',
                  'hierarchy': 16,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_personLookup',
                  'primary_key': 'PERSON_LOOKUP_ID',
                  'delete_last': False
                  },

                  # added on 06-Nov-2018
                 {'oracle_table': 'SA_ALERT_INFO_LOOKUP',
                  'company': 'TVH',
                  'hierarchy': 20,
                  'server': 'TVHA-UH-DB01',
                  'on_prem_database': 'uhlive',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_alertInfoLookup',
                  'primary_key': 'ALERT_INFO_LOOKUP_ID',
                  'delete_last': False
                  },

                 {'oracle_table': 'SA_ALERTS_INFO_MASTER',
                  'company': 'TVH',
                   'hierarchy': 21,
                   'server': 'TVHA-UH-DB01',
                   'on_prem_database': 'uhlive',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_alertInfoMaster',
                   'primary_key': 'ALERTS_INFO_MASTER_ID',
                   'delete_last': False
                   },


                  # ================================== MTH Tables TEST ================================== #
                  {'oracle_table': 'SA_BLOCKS',
                   'company': 'MTH Test',
                   'hierarchy': 107,
                   'server': 'MET-PRD-VM-DB02',
                   'on_prem_database': 'HOUTEST',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_blocks',
                   'primary_key': 'BLOCKS_ID',
                   'delete_last': False
                   },

                   {'oracle_table': 'SA_LOCAL_AUTHORITY',
                    'company': 'MTH Test',
                    'hierarchy': 104,
                    'server': 'MET-PRD-VM-DB02',
                    'on_prem_database': 'HOUTEST',
                    'col_to_increment': 'B_LOADID',
                    'sql_statement': 'SELECT * FROM semarchy_local_authority',
                    'primary_key': 'LOCAL_AUTHORITY_ID',
                    'delete_last': False
                    },

                  {'oracle_table': 'SA_LOOKUP',
                   'company': 'MTH Test',
                   'hierarchy': 101,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_lookup',
                  'primary_key': 'LOOKUP_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_PATCH_LOOKUP',
                   'company': 'MTH Test',
                   'hierarchy': 102,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_patch_lookup',
                  'primary_key': 'PATCH_LOOKUP_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_SCHEMES',
                   'company': 'MTH Test',
                   'hierarchy': 106,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_scheme',
                  'primary_key': 'SCHEMES_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_STAFF_INVOLVEMENT',
                   'company': 'MTH Test',
                   'hierarchy': 105,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_staff',
                  'primary_key': 'STAFF_INVOLVEMENT_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_SUB_BLOCKS',
                   'company': 'MTH Test',
                   'hierarchy': 108,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_subblocks',
                  'primary_key': 'SUB_BLOCKS_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_TENURE_TYPE',
                   'company': 'MTH Test',
                   'hierarchy': 103,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_tenure_types',
                  'primary_key': 'TENURE_TYPE_ID',
                  'delete_last': False
                  },

                  {'oracle_table': 'SA_UNITS',
                   'company': 'MTH Test',
                   'hierarchy': 109,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_units',
                  'primary_key': 'UNITS_ID',
                  'delete_last': False
                  },



                  # 28-Feb-2019
                  {'oracle_table': 'SA_PERSON',
                   'company': 'MTH Test',
                   'hierarchy': 110,
                  'server': 'MET-PRD-VM-DB02',
                  'on_prem_database': 'HOUTEST',
                  'col_to_increment': 'B_LOADID',
                  'sql_statement': 'SELECT * FROM semarchy_person',
                  'primary_key': 'PERSON_ID',
                  'delete_last': False
                  },

                  # {'oracle_table': 'SA_CONTACT_PREFERENCES',
                  #  'company': 'MTH Test',
                  #  'hierarchy': 111,
                  # 'server': 'MET-PRD-VM-DB02',
                  # 'on_prem_database': 'HOUTEST',
                  # 'col_to_increment': 'B_LOADID',
                  # 'sql_statement': 'SELECT * FROM semarchy_contact_preferences',
                  # 'primary_key': 'CONTACT_PREFERENCES_ID',
                  # 'delete_last': False
                  # },
                  #
                  # {'oracle_table': 'SA_COMMUNICATION',
                  #  'company': 'MTH Test',
                  #  'hierarchy': 112,
                  # 'server': 'MET-PRD-VM-DB02',
                  # 'on_prem_database': 'HOUTEST',
                  # 'col_to_increment': 'B_LOADID',
                  # 'sql_statement': 'SELECT * FROM semarchy_communication',
                  # 'primary_key': 'COMMUNICATION_ID',
                  # 'delete_last': False
                  # },
                  #
                  # {'oracle_table': 'SA_VULNERABILITY_DETAILS',
                  #  'company': 'MTH Test',
                  #  'hierarchy': 113,
                  # 'server': 'MET-PRD-VM-DB02',
                  # 'on_prem_database': 'HOUTEST',
                  # 'col_to_increment': 'B_LOADID',
                  # 'sql_statement': 'SELECT * FROM semarchy_vulnerability',
                  # 'primary_key': 'VULNERABILITY_DETAILS_ID',
                  # 'delete_last': False
                  # },


                # ================================== MTH Tables LIVE ================================== #
                  {'oracle_table': 'SA_BLOCKS',
                   'company': 'MTH Live',
                   'hierarchy': 207,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_blocks',
                   'primary_key': 'BLOCKS_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_LOCAL_AUTHORITY',
                   'company': 'MTH Live',
                   'hierarchy': 204,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_local_authority',
                   'primary_key': 'LOCAL_AUTHORITY_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_LOOKUP',
                   'company': 'MTH Live',
                   'hierarchy': 201,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_lookup',
                   'primary_key': 'LOOKUP_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_PATCH_LOOKUP',
                   'company': 'MTH Live',
                   'hierarchy': 202,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_patch_lookup',
                   'primary_key': 'PATCH_LOOKUP_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_SCHEMES',
                   'company': 'MTH Live',
                   'hierarchy': 206,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_scheme',
                   'primary_key': 'SCHEMES_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_STAFF_INVOLVEMENT',
                   'company': 'MTH Live',
                   'hierarchy': 205,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_staff',
                   'primary_key': 'STAFF_INVOLVEMENT_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_SUB_BLOCKS',
                   'company': 'MTH Live',
                   'hierarchy': 208,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_subblocks',
                   'primary_key': 'SUB_BLOCKS_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_TENURE_TYPE',
                   'company': 'MTH Live',
                   'hierarchy': 203,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_tenure_types',
                   'primary_key': 'TENURE_TYPE_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_UNITS',
                   'company': 'MTH Live',
                   'hierarchy': 209,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': 'SELECT * FROM semarchy_units',
                   'primary_key': 'UNITS_ID',
                   'delete_last': False
                   },

                  {'oracle_table': 'SA_RESIDENTS',
                   'company': 'MTH Live',
                   'hierarchy': 210,
                   'server': 'MET-PRD-VM-DB01',
                   'on_prem_database': 'HOULIVE',
                   'col_to_increment': 'B_LOADID',
                   'sql_statement': """SELECT HOUSE_SIZE AS HOUSE_SIZE, 
                                              RESIDENTS_ID AS RESIDENTS_ID, 
                                              HOUSE_REF AS HOUSE_REF, 
                                              AGREEMENT_REF AS AGREEMENT_REF, 
                                              AGREEMENT_DESC AS AGREEMENT_DESC, 
                                              to_date(START_OF_TERM,'DD-MON-RRRR HH:MI:SS') AS START_OF_TERM, 
                                              to_date(END_OF_TERM,'DD-MON-RRRR HH:MI:SS') AS END_OF_TERM, 
                                              CURRENT_OCCUPANT AS CURRENT_OCCUPANT, 
                                              STOCK_GROUP AS STOCK_GROUP, 
                                              OCCUPANCY_TERMINATED AS OCCUPANCY_TERMINATED, 
                                              RENT_VALUE AS RENT_VALUE, 
                                              OCCUPANCY_STATUS AS OCCUPANCY_STATUS, 
                                              CURRENT_BALANCE AS CURRENT_BALANCE, 
                                              SCH_VALUE AS SCH_VALUE, 
                                              RESIDENT_TYPE AS RESIDENT_TYPE, 
                                              F_RENT_GRP_REF AS F_RENT_GRP_REF, 
                                              F_UNITS AS F_UNITS, 
                                              F_PROPERTY_TYPE AS F_PROPERTY_TYPE, 
                                              F_TENURE_TYPE AS F_TENURE_TYPE, 
                                              b_classname AS B_CLASSNAME, 
                                              to_date(B_CREDATE,'DD-MON-RRRR HH:MI:SS') AS B_CREDATE, 
                                              B_CREATOR AS B_CREATOR, 
                                              F_SOURCE_SYSTEM AS F_SOURCE_SYSTEM, 
                                              F_DATA_OWNERSHIP AS F_DATA_OWNERSHIP, 
                                              hash_value AS HASH_VALUE
                                          FROM semarchy_residents""",
                   'primary_key': 'RESIDENTS_ID',
                   'delete_last': False
                   },

              ]


# REGEX - change hierachies from 1xx to 2xx
# replace: 'hierarchy': 1(\d\d)
# with:    'hierarchy': 2\1

# To quickly update the project in GitHub, run the following in the Shell, while being in the 'Semarchy' project:
# git add --all && git commit -m "nothing major" && git push

if __name__ == "__main__":
    from typing import List, Dict, Union
    TVH_tables: List[Dict[str, Union[str, int, bool]]] = [d for d in tables_to_update if d['company'] == 'TVH']
    MTH_Test_tables: List[Dict[str, Union[str, int, bool]]] = [d for d in tables_to_update if d['company'] == 'MTH Test']
    MTH_Live_tables: List[Dict[str, Union[str, int, bool]]] = [d for d in tables_to_update if d['company'] == 'MTH Live']

    print('Total tables:', len(tables_to_update))

    print('TVH tables:', len(TVH_tables))
    print('MTH Test tables:', len(MTH_Test_tables))
    print('MTH Live tables:', len(MTH_Live_tables))