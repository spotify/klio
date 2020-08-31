# WARNING
All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job. 

If you are looking for an example of a working `klio` job, please look in the `klio/examples` directory.


## Integration Test

### Test definition

This test adapts [this public librosa example](https://librosa.org/doc/latest/auto_examples/plot_vocal_separation.html) for separating vocals from instrumental into a klio pipeline.

* Use v2 of configuration (and defacto v2 of klio messages)
* Read klio messages from a local text file
* Using the new transforms in `klio-audio`:
    * Download audio into memory from GCS
    * Load audio from memory into librosa
    * Calculate the short-time fourier transform (STFT)
* Separate vocals from instruments (multiple "user"-implemented transforms)
* Using new klio transforms in `klio-audio`:
    * Generate matplotlib figures (png) for the separation
    * Upload the plots to GCS
* Runs on direct runner

### Expected result

* The output file matches the input file
* No de/serialization issues
* For each entity ID, there are three resulting plots: 
    1. the full magnitude spectrogram
    2. the background spectrogram (instrumental)
    3. the foreground spectrogram (vocal)

### To run

In the top level of this repo:

```sh
$ KLIO_TEST_DIR=audio-spectrograms tox -c integration/tox.ini
```
