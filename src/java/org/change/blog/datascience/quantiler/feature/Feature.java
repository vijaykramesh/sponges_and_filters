package org.change.blog.datascience.quantiler.feature;

import cascading.flow.FlowDef;
import cascading.operation.DebugLevel;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.change.blog.datascience.quantiler.TapFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Feature extends FlowDef implements Serializable {

  protected Map<String, Pipe> countSources;
  protected TapFactory tapFactory;

  public Feature(TapFactory tapFactory) {
    this(tapFactory, DebugLevel.DEFAULT);
  }


  public Feature(TapFactory tapFactory, DebugLevel debugLevel) {
    super();

    setName(getFeatureName());
    setDebugLevel(debugLevel);

    countSources = new HashMap<String, Pipe>();

    this.tapFactory = tapFactory;
  }

  protected void addTailSink(Pipe pipe) {
    addTailSink(pipe, tapFactory.quantiledSink(pipe.getName()));
  }


  public String getFeatureName() {
    return getClass().getSimpleName();
  }

  protected Pipe getCountSource(String name, Fields fields) {
    Pipe source = countSources.get(name);

    if (source == null) {
      source = new Pipe(name);
      super.addSource(source, tapFactory.countSource(name, fields));
      countSources.put(name, source);
    }

    return source;
  }


}
