# WARNING
All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job. 

If you are looking for an example of a working `klio` job, please look in the `klio/examples` directory.

## Integration Test

### Test definition

* Use v2 of configuration (and defacto v2 of klio messages)
* Read klio messages from 2 local text files
* Transform to log element via separate map functions (to show they can be operated on separately)
* Flatten into 1 pcollection
* Write klio messages to 1 local text file
* Runs on direct runner

### Expected result

* The output file matches the input files combined
* No de/serialization issues

### To run

In the top level of this repo:

```sh
$ KLIO_TEST_DIR=multi-event-input-batch tox -c integration/tox.ini
```
