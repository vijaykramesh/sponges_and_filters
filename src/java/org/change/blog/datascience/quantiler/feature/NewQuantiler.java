package org.change.blog.datascience.quantiler.feature;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.change.blog.datascience.quantiler.TapFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Random;

// TODO we might want to make this abstract and explicitly require
// a change.ml.etl.post.<FEATURE> class for each pre feature that
// gets quantiled -- that way we can still use getFeatureName() to
// name the pipes (whereas now it passes the source pipe's name thru)

public class NewQuantiler extends Feature {

  public static final float ETA = 0.001f;
  public static final int DEFAULT_NUM_QUANTILES = 5;
  public static Fields featureFields = new Fields("features");

  String source;
  Fields dataFields, keys;
  int numQuantiles, numBoundaries;

  Fields quantileFields, boundaryFields, joinedFields;

  public Fields fields() {
    return new Fields("user_id", "features");
  }


  public NewQuantiler(
      String source,
      Fields dataFields,
      Fields keys,
      TapFactory tapFactory
  ) {
    this(source, dataFields, keys, tapFactory, DEFAULT_NUM_QUANTILES);
  }

  public NewQuantiler(
      String source,
      Fields dataFields,
      Fields keys,
      TapFactory tapFactory,
      int numQuantiles
  ) {

    super(tapFactory, DebugLevel.DEFAULT);
    setName(source + "_new_quantiled");

    this.source = source;
    this.dataFields = dataFields;
    this.keys = keys;
    this.numQuantiles = numQuantiles;
    numBoundaries = numQuantiles;

    Pipe data = getCountSource(source, keys.append(dataFields));

    Fields compareFields = new Fields(dataFields.get(0));
    compareFields.setComparator(dataFields.get(0), createRandomCompare());

    quantileFields = new Fields();
    boundaryFields = new Fields();
    joinedFields = new Fields();

    for (int f = 0; f < dataFields.size(); f++) {
      String field = (String) dataFields.get(f);

      String[] boundaryFieldNames = new String[numQuantiles];
      String[] quantileFieldNames = new String[numQuantiles];

      for (int q = 0; q < numQuantiles; q++)
        quantileFieldNames[q] = field + "_q" + q;

      for (int b = 0; b < numBoundaries; b++)
        boundaryFieldNames[b] = field + "_b" + b;

      quantileFields = quantileFields.append(new Fields(quantileFieldNames));
      boundaryFields = boundaryFields.append(new Fields(boundaryFieldNames));
    }
    joinedFields = dataFields.append(boundaryFields);


    Pipe boundaries = new Pipe(
        getName() + "_boundaries",
        new Every(
            new GroupBy(
                new Discard(data, keys),
                Fields.NONE,
                compareFields),
            new OnlineAggregator(),
            boundaryFields));

    Pipe appended = new HashJoin(
        data, Fields.NONE,
        boundaries, Fields.NONE,
        keys.append(joinedFields));


    Pipe transformed = new Each(
        appended,
        keys.append(joinedFields),
        new EvaluateBoundaries(),
        keys.append(quantileFields));

    Pipe munged = new Each(
        transformed,
        keys.append(quantileFields),
        new Munger(keys.append(quantileFields)),
        new Fields("user_id", "quantile")
    );

    addTailSink(new Pipe(source + "_new_quantiled", munged));
  }

  protected void addTailSink(Pipe pipe) {
    super.addTailSink(new Pipe(getName(), pipe));
  }


  static class Munger
      extends BaseOperation<Munger.Context>
      implements Function<Munger.Context> {
    class Context {

      String quantile;

      Context() {
        this.quantile = null;
      }

    }


    Munger(Fields fields) {
      super(new Fields("user_id2", "quantile"));
    }

    public void prepare(FlowProcess flow, OperationCall<Context> call) {
      call.setContext(new Context());
    }

    public void operate(FlowProcess flow, FunctionCall<Context> call) {
      Context context = call.getContext();
      TupleEntry args = call.getArguments();

      for (int i = 1; i < args.getFields().size(); i++) {
        if (args.getString(i).equals("1")) {
          Tuple result = new Tuple(2);
          result.clear();
          result.add(args.getString("user_id"));
          result.add(String.valueOf(i - 1));
          call.getOutputCollector().add(result);
        }
      }

    }
  }

  public static RandomCompare createRandomCompare() {
    return new RandomCompare();
  }

  public static class RandomCompare implements Serializable, Comparator {
    public int compare(Object o1, Object o2) {
      Random r = new Random();
      int o = r.nextInt(100);
      int t = r.nextInt(100);
      return o - t;
    }
  }

  class Context {
    float[][] data;
    float[] min, max;
    int numArgs;

    Context(int numArgs) {
      data = new float[numArgs][numBoundaries];

      min = new float[numArgs];
      max = new float[numArgs];
      this.numArgs = numArgs;
    }
  }

  class OnlineAggregator
      extends BaseOperation<Context>
      implements Aggregator<Context> {
    OnlineAggregator() {
      super(boundaryFields);
    }

    // the heart of the beast
    float calculate(float newValue, float aggregate, int boundary) {
      // passed a boundary, but divide over numQuantiles
      return aggregate + ETA * (Math.signum(newValue - aggregate) + 2f * (boundary + 1f) / numQuantiles - 1f);
    }

    public void start(FlowProcess flow, AggregatorCall<Context> call) {
      call.setContext(new Context(call.getArgumentFields().size()));
    }

    public void aggregate(FlowProcess flow, AggregatorCall<Context> call) {
      TupleEntry arguments = call.getArguments();
      Context context = call.getContext();

      for (int a = 0; a < context.numArgs; a++) {
        float value = arguments.getFloat(a);
        if (value == 0f) continue;

        if (context.min[a] == 0f || value < context.min[a]) context.min[a] = value;
        if (value > context.max[a]) context.max[a] = value;

        for (int b = 0; b < numBoundaries; b++) {
          if (context.data[a][b] == 0d) context.data[a][b] = value;
          else {
            context.data[a][b] = calculate(value, context.data[a][b], b);
            // ensure boundary is at least as large as the previous boundary
            if (b > 0 && context.data[a][b] < context.data[a][b - 1])
              context.data[a][b] = context.data[a][b - 1];
          }
        }
      }
    }

    public void complete(FlowProcess flow, AggregatorCall<Context> call) {
      Context context = call.getContext();

      Tuple result = new Tuple();
      for (int a = 0; a < context.numArgs; a++) {
        for (int b = 0; b < numBoundaries; b++) {
          if (context.data[a][b] < context.min[a]) context.data[a][b] = context.min[a];
          if (context.data[a][b] > context.max[a]) context.data[a][b] = context.max[a];
          result.add(context.data[a][b]);
        }
      }

      call.getOutputCollector().add(result);
    }
  }

  class EvaluateBoundaries extends BaseOperation implements Function {
    EvaluateBoundaries() {
      super(new Fields("user_id2").append(quantileFields));
    }

    public void operate(FlowProcess flow, FunctionCall call) {
      TupleEntry arguments = call.getArguments();

      Tuple result = new Tuple();
      result.clear();
      result.add(arguments.get(0));

      for (int i = 0; i < dataFields.size(); i++) {
        String f = (String) dataFields.get(i);

        float featureValue = arguments.getFloat(f);

        int quantile;
        if (featureValue == 0f)
          quantile = -1; // meaning quantile == q will never be true
        else {
          quantile = 0;
          while (
              quantile < (numBoundaries - 1) &&
                  featureValue > arguments.getFloat(new Fields(f + "_b" + quantile))
              ) quantile++;
        }

        for (int q = 0; q < numQuantiles; q++)
          result.add(quantile == q ? 1 : 0);
      }

      call.getOutputCollector().add(result);
    }
  }
}
