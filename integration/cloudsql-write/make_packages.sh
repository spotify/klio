#!/bin/bash
set -x
cd ../../core && rm -rf build dist && python setup.py build sdist bdist_wheel && cp dist/* ../integration/cloudsql-write
cd ../lib && rm -rf build dist && python setup.py build sdist bdist_wheel && cp dist/* ../integration/cloudsql-write
cd ../exec && rm -rf build dist && python setup.py build sdist bdist_wheel && cp dist/* ../integration/cloudsql-write
