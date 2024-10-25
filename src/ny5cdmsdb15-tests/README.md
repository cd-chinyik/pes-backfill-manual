# Powershell Script to Perform PES Backfill for Engage+ Campaign Events on NA Clients

## How it works

- For each CDMSDB instance:
    - Perform health check on CDMSDB instance, skip if unable to connect to the instance.
    - Gets list of active parent cust_id from xyz_cms_common DB in a specified CDMSDB server
    - For each active parent cust_id:
        - Get min and max event IDs from respective event tables based on specified date range
        - If either min or max event ID cannot be found from the table, event will <b>not</b> be backfilled
        - Run bcp command based on min and max event IDs obtained and other specified parameters to generate backfill file
    - When all the backfill files is being generated, please download it manually and pass it to Wei Tyug Lim (ML Team) for her to test on her ESL env.

## How to use

- Configure parameters as needed on these lines in the script

| Parameter              | Description                                                               | Example Value                             |
|------------------------|---------------------------------------------------------------------------|-------------------------------------------|
| **`$cdmsInstances`**     | Array of CDMSDB server instance names.                   | `@("NY5PCDMSDB01", "NY5PCDMSCL01I01/SQL01")`                             |
| **`$startDate`**        | The start date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)         | `[DateTime]"2024-09-01"`                  |
| **`$endDate`**          | The end date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)           | `[DateTime]"2024-10-08"`                  |
| **`$backfillDir`**      | The full path of the directory where the backfill data will be stored.     | `"V:\DMS_Data04\pes_backfill\na"`   |
| **`$batchSize`**         | The number of events to be generated in each backfill file for batching purpose.        | `10000`                         |
