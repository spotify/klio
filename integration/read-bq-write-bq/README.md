# WARNING
All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job. 

If you are looking for an example of a working `klio` job, please look in the `klio/examples` directory.


## Integration Test

* Uses v2 of configuration (and defacto v2 of klio messages)
* Runs on direct runner
* Appends `BUILD_ID` environment variable value to input and output table names
* Generates and reads from an intermediate config file, `klio-job-it.yaml` that contains the updated table names; 
this is set using the `KLIO_CONFIG_FILE` environment variable.

### Test definition

* (setup) Creates `klio-job-it.yaml` file with unique table names & populates the input table
* (job) Reads entity_ids from BQ table
* (job) Logs entity_ids and emits json serialized rows
* (job) Writes rows to BigQuery table 
* (teardown) Removes input and output tables


### Expected result

* The output table exists and contains the expected rows constructed from the input table rows
* No de/serialization issues

### To run

In the top level of this repo:

```sh
$ KLIO_TEST_DIR=read-bq-write-bq BUILD_ID=$USER tox -c integration/tox.ini
```
