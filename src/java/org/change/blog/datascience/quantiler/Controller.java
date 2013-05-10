package org.change.blog.datascience.quantiler;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeDef;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.tuple.Fields;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;


public class Controller extends CascadeDef {

  public static Properties buildProperties(String environment) throws IOException {
    Properties props = new Properties();
    props.load(getResourceAsStream(environment + ".properties"));
    return props;
  }

  public static InputStream getResourceAsStream(String name) {
    return Controller.class.getClassLoader().getResourceAsStream(name);
  }

  public static void main(String[] args) {
    try {

      Properties properties = buildProperties(args[0]);

      if (args.length > 1)
        properties.setProperty("startDate", args[1]);

      Controller controller = new Controller(properties);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  protected Properties properties;
  protected FlowConnector flowConnector;
  protected Map<String, String> fields;

  protected Controller(Properties properties) {
    this.properties = properties;

    TapFactory tapFactory = new TapFactory(properties);
    flowConnector = new HadoopFlowConnector(properties);


    AppProps.setApplicationJarClass(
        properties, Controller.class);


    quantile("signature_counts", new Fields("user_id", "signature_count"), tapFactory);


//    Cascade cascade = new CascadeConnector().connect(this);
//
//    cascade.complete();

  }

  protected void quantile(String sourceName, Fields sourceField, TapFactory tapFactory) {
    System.out.println("Helo World");
  }


  // protected void verifyContinuity(String sourceName, Fields sourceField, Date startDate, TapFactory tapFactory) {
  //   verifyContinuity(sourceName, sourceField, startDate, tapFactory, tapFactory.unfilteredSource(sourceName));
  // }

  // protected void verifyContinuity(String sourceName, Fields sourceField, Date startDate, TapFactory tapFactory, Tap sourceTap) {

  //   Pipe source = new Pipe(sourceName + "/continuity");

  //   source = new Retain(source, sourceField);

  //   source = continuityAggregate(timeBucket(filterDate(source, startDate)));

  //   Map<String, Tap> sinks = new HashMap<String, Tap>();
  //   sinks.put(sourceName + "/continuity", tapFactory.continuitySink(sourceName));

  //   List<Pipe> pipes = new ArrayList<Pipe>();
  //   pipes.add(source);

  //   if (this.thresholds.containsKey(sourceName)) {
  //     Pipe verified = new Pipe(sourceName + "/verified", source);
  //     verified = verifyContinuityAggregate(verified, this.thresholds.get(sourceName));
  //     pipes.add(verified);
  //     sinks.put(sourceName + "/verified", tapFactory.verifiedSink(sourceName));


  //   }

  //   addFlow(flowConnector.connect(
  //           sourceTap,
  //           sinks,
  //           pipes
  //   ));
  // }

  // Pipe timeBucket(Pipe source) {
  //   return new Each(
  //           source,
  //           new Fields("created_at"),
  //           new ContinuityBucketer(),
  //           new Fields("created_at", "time_bucket")
  //   );
  // }

  // Pipe filterDate(Pipe source, Date startDate) {
  //   return new Each(source,
  //           new ExpressionFilter(
  //                   "created_at.compareTo(\"" +
  //                           new SimpleDateFormat(DATE_FORMAT).format(startDate) +
  //                           "\") < 0",
  //                   String.class));
  // }

  // Pipe continuityAggregate(Pipe source) {
  //   return new Every(
  //           new GroupBy(
  //                   source,
  //                   new Fields("time_bucket"),
  //                   new Fields("created_at")
  //           ), new Fields("created_at", "time_bucket"), new ContinuityAggregator(), new Fields("time_bucket", "average_delta", "max_delta"));
  // }

  // Pipe verifyContinuityAggregate(Pipe source, Map<String, Double> thresholds) {
  //   return new Every(
  //           new GroupBy(
  //                   source,
  //                   Fields.NONE
  //           ),
  //           new Fields("time_bucket", "average_delta", "max_delta"), new VerifyContinuityAggregator(thresholds), new Fields("warning_average", "warning_max", "failure_average", "failure_max")
  //   );


  // }
}
