package com.svsg.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class SimpleUDF extends UDF {

    public Text evaluate(Text input) {
        return new Text("Dr. " + input.toString());
    }
}