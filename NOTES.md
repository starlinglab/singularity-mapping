# Notes

## Data layout
- One CAR has the DAG
- All the rest have data
- Data CARs are full of 1M blocks of raw data (identified by raw CIDs)
- They also have periodic dag-pb sections
  - Those have a root CID of the dag-pb type
  - And then links to each of the CIDs of the 1M blocks
  - In Unixfs file format I think
- Each of those dag-pb sections corresponds to 1G of data (or less for the last chunk of a file)
  - So the root CID for those sections can be found in the `file_ranges` table
- Except if the file is small (under 1M? 1G?)
  - Then the CID in `file_ranges` is raw
  - And it's stored just as a raw block/CID in the CAR

## Plan
- Collect all the file_range CIDs
- For each CAR, get all the CIDs
- Store every match in a DB table
- Store as mapping between file_range_id and car_id
