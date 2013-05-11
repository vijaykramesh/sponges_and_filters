package org.change.blog.datascience.quantiler;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeDef;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.tuple.Fields;
import org.change.blog.datascience.quantiler.util.Feature;
import org.change.blog.datascience.quantiler.util.NewQuantiler;
import org.change.blog.datascience.quantiler.util.OldQuantiler;

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

      Controller controller = new Controller(properties, args[1], args[2], args[3]);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  protected Properties properties;
  protected FlowConnector flowConnector;
  protected Map<String, String> fields;

  protected Controller(Properties properties, String whichRound, String whichSource, String numFiles) {
    this.properties = properties;

    TapFactory tapFactory = new TapFactory(properties);
    flowConnector = new HadoopFlowConnector(properties);


    AppProps.setApplicationJarClass(
        properties, Controller.class);

    if (whichRound.equals("old") || whichRound.equals("both")){
      addFlow(flowConnector.connect(new OldQuantiler(whichSource, new Fields("signature_count_0d"), new Fields("user_id"), tapFactory)));
    }

    if(whichRound.equals("new") || whichRound.equals("both")){
      addFlow(flowConnector.connect(new NewQuantiler(whichSource, new Fields("signature_count_0d"), new Fields("user_id"), tapFactory)));
    }



    Cascade cascade = new CascadeConnector().connect(this);

    cascade.complete();

  }


}
