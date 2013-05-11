package org.change.blog.datascience.quantiler.util;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.pipe.*;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.lang.StringUtils;
import org.change.blog.datascience.quantiler.TapFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class OldQuantiler extends Feature{
  public static final int DEFAULT_DOWNSAMPLE_TARGET = 10000;
  public static final int DEFAULT_NUM_QUANTILES = 5;

  static final String BOUNDARY_SEPARATOR_CHARACTER = ",";

  static class Stash {
    Tuple result;
    Random random;

    Stash(int tupleSize) {
      this(tupleSize, null);
    }

    Stash(int tupleSize, Long seed) {
      this.result = Tuple.size(tupleSize);
      this.random = (seed == null ? new Random() : new Random(seed));
    }
  }

  static class Counter extends SubAssembly {
    static final int DEFAULT_BREAKDOWN_FACTOR = 65536;

    class RandomKeyGenerator
        extends BaseOperation<Stash>
        implements cascading.operation.Function<Stash>
    {
      int breakdownFactor;

      RandomKeyGenerator() {
        this(DEFAULT_BREAKDOWN_FACTOR);
      }

      RandomKeyGenerator(int breakdownFactor) {
        super(0, new Fields("random_key"));
        this.breakdownFactor = breakdownFactor;
      }

      public void prepare(FlowProcess flow, OperationCall<Stash> call) {
        call.setContext(new Stash(getFieldDeclaration().size()));
      }

      public void operate(FlowProcess flow, FunctionCall<Stash> call) {
        Tuple result = call.getContext().result;
        Random random = call.getContext().random;

        result.clear();
        result.add(random.nextInt() % breakdownFactor);

        call.getOutputCollector().add(result);
      }
    }

    Counter(Pipe data) {
      // Annotate each value with a random int between 0 and DEFAULT_BREAKDOWN_FACTOR,
      // then group over that random number and count - this is purely for performance
      // reasons, so we can distribute our data into small enough chunks to fit into a
      // single aggregator call.

      String name = data.getName();

      Pipe randomKeys = new Each( data,
          Fields.NONE,
          new RandomKeyGenerator(),
          new Fields("random_key") );

      Pipe firstCount = new CountBy(
          randomKeys,
          new Fields("random_key"),
          new Fields("first_count") );

      firstCount = new Retain(firstCount, new Fields("first_count"));

      Pipe totalCount = new SumBy(
          "totalCount",
          firstCount,
          Fields.NONE, // grouping by Fields.NONE groups all the values under a single aggregator call
          new Fields("first_count"),
          new Fields("total_count"),
          Integer.TYPE );

      totalCount = new Retain(totalCount, new Fields("total_count"));

      setTails( new Pipe( name,
          new HashJoin(
              new Checkpoint(totalCount),
              Fields.NONE,
              data,
              Fields.NONE ) ) );
    }
  }

  static class Downsampler
      extends BaseOperation<Stash>
      implements Function<Stash>
  {
    int downsampleTarget;
    Long randomSeed;

    // Expected input fields: (total_count, value)
    // in that order, but not necc. with those names
    // NB: total_count is the same for all fields,
    // and is the total number of records in the set
    // we're downsampling. Check out OldQuantiler.Counter.
    Downsampler(int downsampleTarget) {
      this(downsampleTarget, null);
    }

    Downsampler(int downsampleTarget, Long randomSeed) {
      super(2, new Fields("downsampled"));
      this.downsampleTarget = downsampleTarget;
      this.randomSeed = randomSeed;
    }

    public void prepare(FlowProcess flow, OperationCall<Stash> call) {
      int size = getFieldDeclaration().size();
      Stash stash = (
          randomSeed == null
              ? new Stash(size)
              : new Stash(size, randomSeed) );

      call.setContext(stash);
    }

    public void operate(FlowProcess flow, FunctionCall<Stash> call) {
      TupleEntry arguments = call.getArguments();

      Tuple result = call.getContext().result;
      Random random = call.getContext().random;

      float recordCount = arguments.getFloat(0);
      // should be an int, but fetch as float so we use
      // FP division

      if (random.nextFloat() < (downsampleTarget / recordCount)) {
        result.clear();
        result.add(arguments.getString(1));
        call.getOutputCollector().add(result);
      }
    }
  }

  static class FloatFilter extends BaseOperation<Tuple> implements Function<Tuple> {
    FloatFilter() {
      super(1, new Fields("filtered"));
    }

    public void prepare(FlowProcess flow, OperationCall<Tuple> call) {
      call.setContext(Tuple.size(getFieldDeclaration().size()));
    }

    public void operate(FlowProcess flow, FunctionCall<Tuple> call) {
      TupleEntry args = call.getArguments();
      Tuple result = call.getContext();

      result.clear();
      result.add(args.getFloat(0));

      call.getOutputCollector().add(result);
    }
  }

  static class Demarcator
      extends BaseOperation<Demarcator.Context>
      implements Aggregator<Demarcator.Context>
  {
    class Context {
      Tuple result;
      Integer totalCount;
      List<Float> values;

      Context() {
        this.result = Tuple.size(1);
        values = new ArrayList<Float>();
      }

      void aggregate(float value) {
        values.add(value);
      }

      float midpoint(float low, float high) {
        return low + (high - low) / 2;
      }

      float midpointFor(int numQuantiles, int quantileIndex) {
        if (quantileIndex == numQuantiles - 1)
          throw new RuntimeException(
              // much simpler and more flexible to indicate internal boundaries only:
              // no (end) boundary conditions at all! everything off the end of the
              // list in either direction is just in the highest or lowest bucket
              "only run this function through numQuantiles - 2: boundary array denotes internal boundaries only!" );

        if (totalCount == null)
          throw new RuntimeException("Must set totalCount first!");

        int quantileSize = totalCount / numQuantiles;
        // dropping remainder for great not having to special case that shit:
        // totalCount should >> numQuantiles
        int arrayIndex = ((quantileIndex + 1) * quantileSize) - 1;
        return midpoint(values.get(arrayIndex), values.get(arrayIndex + 1));
      }

      Float[] boundaries(int numQuantiles) {
        Collections.sort(values);
        Float[] boundaries = new Float[numQuantiles - 1];

        for (int i = 0; i < boundaries.length; i++)
          boundaries[i] = midpointFor(numQuantiles, i);

        return boundaries;
      }

      Tuple calculateResult(int numQuantiles) {
        result.clear(); // not necessary, I think
        result.add(
            StringUtils.join(boundaries(numQuantiles),
                BOUNDARY_SEPARATOR_CHARACTER) );
        return result;
      }
    }

    int numQuantiles;

    Demarcator() {
      this(DEFAULT_NUM_QUANTILES);
    }

    Demarcator(int numQuantiles) {
      super(2, new Fields("boundaries"));
      this.numQuantiles = numQuantiles;
    }

    public void start(FlowProcess flow, AggregatorCall<Context> call) {
      call.setContext(new Context());
    }

    public void aggregate(FlowProcess flow, AggregatorCall<Context> call) {
      Context context = call.getContext();
      TupleEntry args = call.getArguments();

      if (context.totalCount == null)
        // Kinda shitty to have to pass this on each and every tuple in the stream
        // just to read and store it once
        context.totalCount = args.getInteger(0);

      context.aggregate(args.getFloat(1));
    }

    public void complete(FlowProcess flow, AggregatorCall<Context> call) {
      call.getOutputCollector().add(
          call.getContext().calculateResult(numQuantiles) );
    }
  }

  // Expects: (boundaries, float)
  // where boundaries is the output of Demarcator, above
  static class Replacer
      extends BaseOperation<Replacer.Context>
      implements Function<Replacer.Context>
  {
    class Context {
      float[] boundaries;
      Tuple result;
      int numQuantiles;

      Context(int numQuantiles) {
        this.numQuantiles = numQuantiles;
        this.result = new Tuple();
      }

      void setBoundaries(String boundariesString) {
        String[] split = StringUtils.split(
            boundariesString,
            BOUNDARY_SEPARATOR_CHARACTER );

        boundaries = new float[split.length];

        for (int i = 0; i < split.length; i++)
          boundaries[i] = new Float(split[i]);
      }

      int quantile(float value) {
        int quantile = 0;

        while (quantile < boundaries.length && value > boundaries[quantile])
          quantile++;

        return quantile;
      }

      Tuple result(float value) {
        result.clear();
        int quantile = quantile(value);
        for (int i = 0; i < numQuantiles; i++)
          result.add(quantile == i ? 1 : 0);
        return result;
      }
    }

    static Fields outputSelector(Fields base, int numQuantiles) {
      return outputSelector((String) base.get(0), numQuantiles);
    }

    static Fields outputSelector(String base, int numQuantiles) {
      String[] names = new String[numQuantiles];
      for (int i = 0; i < numQuantiles; i++)
        names[i] = base + "_q" + i;
      return new Fields(names);
    }

    Replacer(Fields field) {
      this(field, DEFAULT_NUM_QUANTILES);
    }

    Replacer(Fields field, int numQuantiles) {
      super(2, outputSelector(field, numQuantiles));
    }

    public void prepare(FlowProcess flow, OperationCall<Context> call) {
      call.setContext(new Context(getFieldDeclaration().size()));
    }

    public void operate(FlowProcess flow, FunctionCall<Context> call) {
      Context context = call.getContext();
      TupleEntry args = call.getArguments();

      if (context.boundaries == null)
        context.setBoundaries(args.getString(0));

      call.getOutputCollector().add(
          context.result(args.getFloat(1)) );
    }
  }
  static class Munger
      extends BaseOperation<Munger.Context>
      implements Function<Munger.Context>
  {
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

      for (int i =1; i < args.getFields().size(); i++){
        if (args.getString(i).equals("1")){
          Tuple result = new Tuple(2);
          result.clear();
          result.add(args.getString("user_id"));
          result.add(String.valueOf(i - 1));
          call.getOutputCollector().add(result);
        }
      }

    }
  }

  String source;
  Fields field;
  Fields fields;
  Long randomSeed;

  public Fields fields(){
    return new Fields("user_id", "feature_name", "feature_value");
  }

  public OldQuantiler(String source, Fields baseField, Fields keys, TapFactory tapFactory) {
    this(source, baseField, keys, tapFactory, DEFAULT_NUM_QUANTILES, DEFAULT_DOWNSAMPLE_TARGET, null);
  }

  public OldQuantiler(
      String source,
      Fields baseField,
      Fields keys,
      TapFactory tapFactory,
      int numQuantiles,
      int downsampleTarget,
      Long randomSeed
  ) {
    super(tapFactory, DebugLevel.DEFAULT);
    setName(source + "/old_quantiled");

    if (baseField.size() != 1)
      throw new IllegalArgumentException(
          "field.size() must == 1 !" );

    // TODO: Boo. Fix this. Probs by making featureDefinition something more sensible, and having it encapsulate
    // the fact that it refers to a particular timeBucket range, for example. Separate class? Just have the fD
    // generate the Feature instances? I think that last one would make more sense.

    // Job -> new TapFactory(Properties) -> new FeatureDictionary -> setup method that iterates over FDict, instantiates
    // Features, and then uses the TapFactory to bind the Feature's registered sinks and sources to Taps, then call
    // Job.addFeature()
    if (source.contains("timeBucketed"))
      this.field = new Fields(baseField.get(0) + "_" + source.split("_")[1]);
    else this.field = baseField;

    this.source = source;
    this.randomSeed = randomSeed;
    this.fields = keys.append(field);

    Fields totalCount = new Fields("total_count");


    Pipe data = getCountSource(source, fields );
    Pipe retained = new Retain(data, fields);

    // Have to filter out the bad values ourselves, cause our first otherwise-necessary attempt to parse any numeric
    // values out happens inside an aggregation call following a GroupBy Fields.NONE, meaning that everything is crammed
    // into that single aggregation call and will thus kill the whole pipe.

    // We rename the pipes on the inside (start) of each operation
    // so the names correspond to the process that would've thrown
    // the error when we look at the trap
    Pipe filtered =
        new Rename(
            new Each(
                new Pipe(source + "/filtered", retained),
                field,
                new FloatFilter(),
                keys.append(new Fields("filtered")) ),
            new Fields("filtered"),
            field );
//    addTailSink(new Pipe(source + "_filtered", filtered));

    // We have to count both before and after the downsampler
    Pipe downsampled =
        new Counter( new Rename(
            new Each(
                new Counter(new Pipe(source + "/downsampled", filtered)),
                totalCount.append(field),
                new Downsampler(downsampleTarget, randomSeed),
                new Fields("downsampled") ),
            new Fields("downsampled"), field ) );
//    addTailSink(new Pipe(source + "_downsampled", downsampled));

    Pipe demarcated =
        new Every(
            new GroupBy(
                new Pipe(source + "/demarcated", downsampled),
                Fields.NONE ),
            new Fields("total_count").append(field),
            new Demarcator(numQuantiles),
            new Fields("boundaries") );
//    addTailSink(new Pipe(source + "_demarcated", demarcated));

    Pipe withBoundaries = new Pipe(source + "/withBoundaries",
        new HashJoin(
            new Checkpoint(demarcated),
            Fields.NONE,
            filtered,
            Fields.NONE ) );
//    addTailSink(new Pipe(source + "_boundaries", withBoundaries));

    this.fields = keys.append(Replacer.outputSelector(field, numQuantiles));

    Pipe replaced = new Each(
        new Pipe(source + "/replaced", withBoundaries),
        new Fields("boundaries").append(field),
        new Replacer(field, numQuantiles),
        fields);

//    addTailSink(new Pipe(source + "_replaced", replaced));

    Pipe munged = new Each(
        replaced,
        fields,
        new Munger(fields),
        new Fields("user_id", "quantile")
    );

    addTailSink(new Pipe(source + "_old_quantiled", munged));
  }
}