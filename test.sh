#!/usr/bin/env bash
mvn clean package
scp -r  target/big-data-analysis-1.0.0.jar pub_dc_appstore@58.215.179.202:~/iot/lib

