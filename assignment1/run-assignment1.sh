#!/bin/sh

hadoop jar dist/cloud9-1.4.7.jar edu.umd.cloud9.example.simple.DemoWordCount \ 
-libjars dist/cloud9-1.4.7.jar,lib/guava-13.0.1.jar -input bible+shakes.nopunc.gz \
-output rmardiko -numReducers 5
