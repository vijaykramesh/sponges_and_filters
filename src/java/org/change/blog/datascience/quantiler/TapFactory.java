package org.change.blog.datascience.quantiler;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.Properties;


public class TapFactory implements Serializable {

  static protected Scheme tsv(Fields fields) {
    TextDelimited scheme = new TextDelimited(fields, true, true, "\t");
    scheme.setNumSinkParts(1);
    return scheme;
  }


  protected String dataRoot;

  protected TapFactory() {
  }

  public TapFactory(Properties properties) {
    dataRoot = properties.getProperty("data.root");
  }

  public String path(String... args) {
    return Joiner.on("/").join(args);
  }

  public Tap countSource(String name, Fields fields) {
    return new Hfs(
        tsv(fields),
        path(dataRoot, name));
  }


  public Tap quantiledSink(String name, Fields fields) {
    return new Hfs(
        tsv(fields),
        path(dataRoot, "quantiled", name));
  }

}
