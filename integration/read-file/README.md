# WARNING
All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job. 

If you are looking for an example of a working `klio` job, please look in the `klio/examples` directory.


## Integration Test

### Test definition

* Use v2 of configuration (and defacto v2 of klio messages)
* Read klio messages from a local text file
* Transform to log element and return (no downloading/uploading of audio files)
* Has no event outputs or data in/outputs

### Expected result

* The output file matches the input file
* No de/serialization issues

### To run

In the top level of this repo:

```sh
$ KLIO_TEST_DIR=read-file tox -c integration/tox.ini
```
