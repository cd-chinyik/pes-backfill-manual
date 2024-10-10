# PES Backfill Script for all customers in a CDMSDB server

######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define PES Event Types
### Supported PES Event Types: all(all events), launch, send, open, click, unsubscribe, hardBounce, softBounce, webEvent, inbound
$pesEvents = "all"
### Define CDMSDB Server Instance Name
$cdmsInstance = "localhost"
### Define Backfill Start Date
$startDate = [DateTime]"2024-04-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-02"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
### Define S3 Bucket
$s3Bucket = "pes-cdms-992063009675"
### Define S3 Directory
$s3Dir = "cy-test-manual"
### Define S3 Region (Use "us-east-1" for NA, "eu-west-1" for EMEA, "ap-northeast-1" for Japan)
$s3Region = "us-east-1"

# Create backfill directory if not exist
New-Item -ItemType Directory -Force -Path $backfillDir

##########################################################################
##      Get list of active parent cust_id from xyz_cms_common DB        ##
##########################################################################      
try {
    $custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
    $custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
    if ($custIds.Count -eq 0) {
        exit
    }
} catch {
    Write-Error "Error finding active parent cust_id: $_"
    exit
}

##############################################################################################
##      START BACKFILL PROCESS FOR ALL CUSTS                                                ##
##      STEP 1 - Get min/max event IDs for each PES event type                              ##
##      STEP 2 - Run bcp command to generate backfill csv file for each PES event type      ##
##############################################################################################

# Today's date for backfill filename
$todayDate = Get-Date -Format "yyyy-MM-dd"
$todayTime = Get-Date -Format "HHmmss"

# campaign_publish event
if ($pesEvents -like "*launch*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_cms_cust_$custId"
        $eventQuery = @"
            SELECT MIN(camp_id) AS min_event_id, MAX(camp_id) AS max_event_id
            FROM dbo.t_camp_stat WITH(NOLOCK)
            WHERE merge_setup_time BETWEEN '$startDate' AND '$endDate'
                OR dms_setup_time BETWEEN '$startDate' AND '$endDate'
                OR rts_setup_time BETWEEN '$startDate' AND '$endDate'
                OR inb_setup_time BETWEEN '$startDate' AND '$endDate'
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_campaignPublish_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_launch_camp_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_send event
if ($pesEvents -like "*send*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            ;WITH send_cte AS
            (
                SELECT 
                    MIN(mc.msg_id) AS min_event_id,
                    MAX(mc.msg_id) AS max_event_id
                FROM dbo.t_msg_cold mc WITH (NOLOCK)
                INNER JOIN dbo.t_msg_chunk mck WITH (NOLOCK)
                    ON mc.chunk_id = mck.chunk_id
                WHERE mc.chunk_id IS NOT NULL
                    AND mck.send_real_time BETWEEN '$startDate' AND '$endDate'
                UNION ALL
                SELECT 
                    MIN(mc.msg_id) AS min_event_id,
                    MAX(mc.msg_id) AS max_event_id
                FROM dbo.t_msg_cold mc WITH (NOLOCK)
                INNER JOIN dbo.t_msg_instant_trigger_timing mitt WITH (NOLOCK)
                    ON mc.msg_id = mitt.msg_id
                    AND mc.camp_id = mitt.camp_id
                WHERE mc.chunk_id IS NULL
                    AND mitt.send_real_time BETWEEN '$startDate' AND '$endDate'
            )
            SELECT 
                MIN(min_event_id) AS min_event_id,
                MAX(max_event_id) AS max_event_id
            FROM send_cte
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageSend_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_send_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_open events
if ($pesEvents -like "*open*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(click_id) AS min_event_id,
                MAX(click_id) AS max_event_id
            FROM dbo.t_click WITH (NOLOCK)
            WHERE click_type_id = 100
                AND click_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageOpen_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_open_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_click events
if ($pesEvents -like "*click*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(click_id) AS min_event_id,
                MAX(click_id) AS max_event_id
            FROM dbo.t_click WITH (NOLOCK)
            WHERE click_type_id = 200
                AND click_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageClick_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_click_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_unsubscribe events
if ($pesEvents -like "*unsub*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(unsub_id) AS min_event_id,
                MAX(unsub_id) AS max_event_id
            FROM dbo.t_msg_unsub WITH (NOLOCK)
            WHERE unsub_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageUnsubscribe_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_unsub_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_hard_bounce events
if ($pesEvents -like "*hardBounce*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(b.bounce_id) AS min_event_id,
                MAX(b.bounce_id) AS max_event_id
            FROM dbo.t_msg_bounce b WITH (NOLOCK)
            INNER JOIN dbo.t_bounce_category bc WITH (NOLOCK)
                ON b.category_id = bc.category_id
            WHERE b.bounce_time BETWEEN '$startDate' AND '$endDate'
                AND bc.hard_flag = 1;
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageHardBounce_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_hard_bounce_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_soft_bounce events
if ($pesEvents -like "*softBounce*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(b.bounce_id) AS min_event_id,
                MAX(b.bounce_id) AS max_event_id
            FROM dbo.t_msg_bounce b WITH (NOLOCK)
            INNER JOIN dbo.t_bounce_category bc WITH (NOLOCK)
                ON b.category_id = bc.category_id
            WHERE b.bounce_time BETWEEN '$startDate' AND '$endDate'
                AND bc.hard_flag IN (0, 2);
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageSoftBounce_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_soft_bounce_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_webevent events
if ($pesEvents -like "*webEvent*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(web_event_id) AS min_event_id,
                MAX(web_event_id) AS max_event_id
            FROM dbo.t_web_event_submission WITH (NOLOCK)
            WHERE event_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageWebEvent_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_webevent_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

# message_inbound events
if ($pesEvents -like "*inbound*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(response_id) AS min_event_id,
                MAX(response_id) AS max_event_id
            FROM dbo.t_sms_response WITH (NOLOCK)
            WHERE response_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageInbound_${todayDate}_${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_inbound_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
}

##############################################################
##      UPLOAD BACKFILL FILE TO S3 BUCKET                   ##
##      UPLOADED FIELS WILL BE REMOVED FROM THE SERVER      ##
##############################################################
# $files = Get-ChildItem -Path $backfillDir -File
# foreach ($file in $files) {
#     $s3Key = Join-Path $s3Dir $file.Name
#     try {
#         Write-S3Object -BucketName $s3Bucket -File $file -Key $s3Key -Region $s3Region
#         Remove-Item -Path $file -Force
#     } catch {
#         Write-Error "Error uploading file to S3: $_"
#     }
# }