# Powershell Script to Perform PES Backfill for Engage+ Campaign Events

## How it works

- Gets list of active parent cust_id from xyz_cms_common DB in a specified CDMSDB server
- For each active parent cust_id:
    - Get min and max event IDs from respective event tables based on specified date range
    - If either min or max event ID cannot be found from the table, event will <b>not</b> be backfilled
    - Run bcp command based on min and max event IDs obtained and other specified parameters to generate backfill file
- Finally, all the backfill files will be uploaded to specified S3 bucket and all the backfill files will be deleted afterwards

## How to use

- Configure parameters as needed on these lines in the script

| Parameter              | Description                                                               | Example Value                             |
|------------------------|---------------------------------------------------------------------------|-------------------------------------------|
| **`$cdmsInstance`**     | CDMSDB server instance name.                  | `"NY5PCDMSDBXX"`                             |
| **`$startDate`**        | The start date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)         | `[DateTime]"2024-09-01"`                  |
| **`$endDate`**          | The end date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)           | `[DateTime]"2024-10-08"`                  |
| **`$backfillDir`**      | The full path of the directory where the backfill data will be stored.     | `"V:\DMS_Data04\pes_backfill\na"`   |
| **`$batchSize`**         | The number of events to be generated in each backfill file for batching purpose.        | `50000`                         |

## Additional Notes:

- These scripts will not upload files to ESL S3 buckets directly, instead they should be manually download first and then pass to Wei Tyug Lim (ML Team) for her to perform her testing on ESL env against prod data