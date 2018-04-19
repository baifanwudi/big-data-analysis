#!/usr/bin/env bash
mvn clean package
scp -r  target/big-data-analysis-1.0.0.jar ct_iot@58.215.179.202:~/iot/lib

