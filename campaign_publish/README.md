# Powershell Script to Perform PES Backfill for Engage+ campaign_publish Events

## How it works

- Gets list of active parent cust_id from xyz_cms_common DB in a specified CDMSDB server
- For each active parent cust_id:
    - Get min and max event IDs from campaign launch event table (`t_camp_stat`) based on specified date range
    - If either min or max event ID cannot be found from the table, event will <b>not</b> be backfilled
    - Run bcp command based on min and max event IDs obtained and other specified parameters to generate backfill file
- Finally, all the backfill files will be uploaded to specified S3 bucket and all the backfill files will be deleted afterwards

## How to use

- Configure parameters as needed on these lines in the script

| Parameter              | Description                                                               | Example Value                             |
|------------------------|---------------------------------------------------------------------------|-------------------------------------------|
| **`$cdmsInstance`**     | The name or address of the CDMS database server instance.                  | `"localhost"`                             |
| **`$startDate`**        | The start date for the backfill operation (in `DateTime` format).          | `[DateTime]"2024-09-01"`                  |
| **`$endDate`**          | The end date for the backfill operation (in `DateTime` format).            | `[DateTime]"2024-10-08"`                  |
| **`$pesRegion`**        | The PES region. Options: `"na"` for North America, `"emea"` for EMEA, `"jpn"` for Japan. | `"na"`                                    |
| **`$backfillDir`**      | The full path of the directory where the backfill data will be stored.     | `"E:\xyz_data\dms\pes_backfill\manual"`   |
| **`$s3Bucket`**         | The name of the S3 bucket where the backfill data will be uploaded.        | `"esl-ue1-dev01"`                         |
- Run the script

### Additional Details:
- The backfill directory is created if it does not already exist.
- Make sure the `$s3Bucket` and `$s3Region` match the configuration of your S3 bucket for the backfill upload process.
