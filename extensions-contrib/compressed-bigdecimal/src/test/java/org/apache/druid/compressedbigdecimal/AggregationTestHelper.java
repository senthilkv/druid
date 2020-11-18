/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.compressedbigdecimal;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class provides general utility to test any druid aggregation implementation given raw data, parser spec,
 * aggregator specs and a group-by query. It allows you to create index from raw data, run a group by query on it which
 * simulates query processing inside of a druid cluster exercising most of the features from aggregation and returns the
 * results that you could verify.
 *
 * @param <T> Type of result to generate
 */
public class AggregationTestHelper<T> extends InitializedNullHandlingTest
{
  private final ObjectMapper mapper;
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;
  private final QueryToolChest<T, ? extends Query<T>> toolChest;
  private final QueryRunnerFactory<T, ? extends Query<T>> factory;

  private final TemporaryFolder tempFolder;

  /**
   * Constructor.
   *
   * @param mapper                object mapper
   * @param indexMerger           indexMerger
   * @param indexIO               indexIO
   * @param toolchest             tool chest
   * @param factory               query runner factory
   * @param tempFolder            temporary folder
   * @param jsonModulesToRegister jsonModulesToRegister
   */
  private AggregationTestHelper(ObjectMapper mapper, IndexMerger indexMerger, IndexIO indexIO,
                                QueryToolChest<T, ? extends Query<T>> toolchest, QueryRunnerFactory<T, ? extends Query<T>> factory,
                                TemporaryFolder tempFolder, List<? extends Module> jsonModulesToRegister)
  {
    this.mapper = mapper;
    this.indexMerger = indexMerger;
    this.indexIO = indexIO;
    this.toolChest = toolchest;
    this.factory = factory;
    this.tempFolder = tempFolder;

    for (Module mod : jsonModulesToRegister) {
      mapper.registerModule(mod);
    }
  }

  /**
   * Group by query aggregation test helper.
   *
   * @param jsonModulesToRegister jsonModulesToRegister
   * @param config                GroupByQueryConfig instance
   * @param tempFolder            temporary folder
   * @return new instance of AggregationTestHelper
   */
  public static final AggregationTestHelper<ResultRow> createGroupByQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister, GroupByQueryConfig config, TemporaryFolder tempFolder)
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    Pair<GroupByQueryRunnerFactory, Closer> factory =
        GroupByQueryRunnerTest.makeQueryRunnerFactory(mapper, config);

    IndexIO indexIO = new IndexIO(mapper, () -> {
      return 0;
    });

    return new AggregationTestHelper<ResultRow>(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        factory.lhs.getToolchest(),
        factory.lhs,
        tempFolder,
        jsonModulesToRegister
    );
  }

  /**
   * Timeseries query aggregation test helper.
   *
   * @param jsonModulesToRegister jsonModulesToRegister
   * @param tempFolder            Temporary folder
   * @return instance of AggregationTestHelper
   */
  public static final AggregationTestHelper<Result<TimeseriesResultValue>> createTimeseriesQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister, TemporaryFolder tempFolder)
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    QueryToolChest<Result<TimeseriesResultValue>, ? extends Query<Result<TimeseriesResultValue>>> toolchest =
        new TimeseriesQueryQueryToolChest();

    QueryRunnerFactory<Result<TimeseriesResultValue>, ? extends Query<Result<TimeseriesResultValue>>> factory =
        new TimeseriesQueryRunnerFactory((TimeseriesQueryQueryToolChest) toolchest, new TimeseriesQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER);

    IndexIO indexIO = new IndexIO(mapper, () -> {
      return 0;
    });

    return new AggregationTestHelper<Result<TimeseriesResultValue>>(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister);
  }

  /**
   * TopN query aggregation test helper.
   *
   * @param jsonModulesToRegister jsonModulesToRegister
   * @param tempFolder            Temporary folder
   * @return instance of AggregationTestHelper
   */
  public static final AggregationTestHelper<Result<TopNResultValue>> createTopNQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister, TemporaryFolder tempFolder)
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    QueryToolChest<Result<TopNResultValue>, ? extends Query<Result<TopNResultValue>>> toolchest =
        new TopNQueryQueryToolChest(new TopNQueryConfig());

    QueryRunnerFactory<Result<TopNResultValue>, ? extends Query<Result<TopNResultValue>>> factory =
        new TopNQueryRunnerFactory(
            new StupidPool<>("TopNQueryRunnerFactory-bufferPool", new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(10 * 1024 * 1024);
              }
            }), (TopNQueryQueryToolChest) toolchest, QueryRunnerTestHelper.NOOP_QUERYWATCHER);

    IndexIO indexIO = new IndexIO(mapper, () -> {
      return 0;
    });

    return new AggregationTestHelper<Result<TopNResultValue>>(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister);
  }

  /**
   * Index and run query on segments.
   *
   * @param inputDataFile    inputDataFile
   * @param parserJson       parserJson
   * @param aggregators      aggregators
   * @param minTimestamp     minTimestamp
   * @param gran             gran
   * @param maxRowCount      maxRowCount
   * @param groupByQueryJson groupByQueryJson
   * @return runQueryOnSegments
   * @throws Exception Exception
   */
  public Sequence<T> createIndexAndRunQueryOnSegment(File inputDataFile, String parserJson, String aggregators,
                                                     long minTimestamp, Granularity gran, int maxRowCount, String groupByQueryJson) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataFile, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  /**
   * Index and run query on segments.
   *
   * @param inputDataStream  inputDataStream
   * @param parserJson       parserJson
   * @param aggregators      aggregators
   * @param minTimestamp     minTimestamp
   * @param gran             gran
   * @param maxRowCount      maxRowCount
   * @param groupByQueryJson groupByQueryJson
   * @return runQueryOnSegments
   * @throws Exception Exception
   */
  public Sequence<T> createIndexAndRunQueryOnSegment(InputStream inputDataStream, String parserJson,
                                                     String aggregators, long minTimestamp, Granularity gran, int maxRowCount, String groupByQueryJson)
      throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataStream, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  /**
   * Create an index.
   *
   * @param inputDataFile inputDataFile
   * @param parserJson    parserJson
   * @param aggregators   aggregators
   * @param outDir        outDir
   * @param minTimestamp  minTimestamp
   * @param gran          gran
   * @param maxRowCount   maxRowCount
   * @throws Exception Exception
   */
  public void createIndex(File inputDataFile, String parserJson, String aggregators, File outDir, long minTimestamp,
                          Granularity gran, int maxRowCount) throws Exception
  {
    createIndex(new FileInputStream(inputDataFile), parserJson, aggregators, outDir, minTimestamp, gran,
        maxRowCount);
  }

  /**
   * Create an index.
   *
   * @param inputDataStream inputDataStream
   * @param parserJson      parserJson
   * @param aggregators     aggregators
   * @param outDir          outDir
   * @param minTimestamp    minTimestamp
   * @param gran            gran
   * @param maxRowCount     maxRowCount
   * @throws Exception Exception
   */
  public void createIndex(InputStream inputDataStream, String parserJson, String aggregators, File outDir,
                          long minTimestamp, Granularity gran, int maxRowCount) throws Exception
  {
    try {
      StringInputRowParser parser = mapper.readValue(parserJson, StringInputRowParser.class);

      LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
      List<AggregatorFactory> aggregatorSpecs =
          mapper.readValue(aggregators, new TypeReference<List<AggregatorFactory>>()
          {
          });

      createIndex(iter, parser, aggregatorSpecs.toArray(new AggregatorFactory[0]), outDir, minTimestamp, gran,
          true, maxRowCount);
    } 
    finally {
      Closeables.close(inputDataStream, true);
    }
  }

  /**
   * Create an index.
   *
   * @param rows                      rows
   * @param parser                    parser
   * @param metrics                   metrics
   * @param outDir                    outDir
   * @param minTimestamp              minTimestamp
   * @param gran                      gran
   * @param deserializeComplexMetrics deserializeComplexMetrics
   * @param maxRowCount               maxRowCount
   * @throws Exception Exception
   */
  @SuppressWarnings({"unchecked", "checkstyle:cyclomaticcomplexity", "checkstyle:parameternumber"})
  public void createIndex(
      Iterator<? extends Object> rows,
      InputRowParser<ByteBuffer> parser,
      final AggregatorFactory[] metrics,
      File outDir,
      long minTimestamp,
      Granularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount) throws Exception
  {

    List<File> toMerge = new ArrayList<>();

    while (rows.hasNext()) {
      try (
          IncrementalIndex<Aggregator> index = new IncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(minTimestamp)
                      .withQueryGranularity(gran)
                      .withMetrics(metrics)
                      .build()
              )
              .setDeserializeComplexMetrics(deserializeComplexMetrics)
              .setMaxRowCount(maxRowCount)
              .buildOnheap()) {
        while (rows.hasNext() && index.canAppendRow()) {
          Object row = rows.next();
          if (row instanceof String && parser instanceof StringInputRowParser) {
            // Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer>
            // as opposed to InputRowsParser<String>
            index.add(((StringInputRowParser) parser).parse((String) row));
          } else {
            index.add(parser.parse((ByteBuffer) row));
          }
        }

        try (
            IncrementalIndex<Aggregator> combinerIndex = new IncrementalIndex.Builder()
                .setIndexSchema(
                    new IncrementalIndexSchema.Builder()
                        .withMinTimestamp(minTimestamp)
                        .withQueryGranularity(gran)
                        .withMetrics(metrics)
                        .build()
                )
                .setDeserializeComplexMetrics(deserializeComplexMetrics)
                .setMaxRowCount(Integer.MAX_VALUE)
                .buildOnheap()) {
          Iterator<Row> rowIter = index.iterator();
          while (rowIter.hasNext()) {
            MapBasedRow r = (MapBasedRow) rowIter.next();
            List<String> dimensions = index.getDimensionNames();
            InputRow ir = new MapBasedInputRow(r.getTimestamp(), dimensions, r.getEvent());
            combinerIndex.add(ir);
          }
          File tmp = tempFolder.newFolder();
          toMerge.add(tmp);
          indexMerger.persist(combinerIndex, tmp, new IndexSpec(), null);
        }
      }
    }

    if (toMerge.size() > 0) {
      List<QueryableIndex> indexes = new ArrayList<>(toMerge.size());
      for (File file : toMerge) {
        indexes.add(indexIO.loadIndex(file));
      }
      indexMerger.mergeQueryableIndex(indexes, true, metrics, outDir, new IndexSpec(), null);

      for (QueryableIndex qi : indexes) {
        qi.close();
      }
    }
  }

  /**
   * Simulates running group-by query on individual segments as historicals would do, json serialize the results
   * from each segment, later deserialize and merge and finally return the results.
   *
   * @param segmentDirs segmentDirs
   * @param queryJson   queryJson
   * @return runQueryOnSegments
   * @throws Exception Exception
   */
  @SuppressWarnings("unchecked")
  public Sequence<T> runQueryOnSegments(final List<File> segmentDirs, final String queryJson) throws Exception
  {
    return runQueryOnSegments(segmentDirs, mapper.readValue(queryJson, Query.class));
  }

  /**
   * Run query on segments.
   *
   * @param segmentDirs segmentDirs
   * @param query       query
   * @return runQueryOnSegmentsObjs
   */
  public Sequence<T> runQueryOnSegments(final List<File> segmentDirs, final Query<T> query)
  {
    final List<Segment> segments = Lists.transform(segmentDirs, segmentDir -> {
      try {
        return new QueryableIndexSegment(indexIO.loadIndex(segmentDir), SegmentId.dummy(""));
      } 
      catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    });

    try {
      return runQueryOnSegmentsObjs(segments, query);
    } 
    finally {
      for (Segment segment : segments) {
        CloseQuietly.close(segment);
      }
    }
  }

  /**
   * Run query on segments.
   *
   * @param segments segments
   * @param query    query
   * @return FinalizeResultsQueryRunner
   */
  public Sequence<T> runQueryOnSegmentsObjs(final List<Segment> segments, final Query<T> query)
  {
    @SuppressWarnings("unchecked") final FinalizeResultsQueryRunner<T> baseRunner =
        new FinalizeResultsQueryRunner<T>(toolChest.postMergeQueryDecoration(toolChest.mergeResults(
             toolChest.preMergeQueryDecoration(factory.mergeRunners(Execs.directExecutor(),
                Lists.transform(segments, segment -> {
                  try {
                    return makeStringSerdeQueryRunner(mapper, (QueryToolChest<T, Query<T>>) toolChest, query, factory.createRunner(segment));
                  } 
                  catch (Exception ex) {
                    throw Throwables.propagate(ex);
                  }
                }))))),
            (QueryToolChest<T, Query<T>>) toolChest);

    return baseRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
  }

  /**
   * Serde query runner.
   *
   * @param mapper     mapper
   * @param toolChest  toolChest
   * @param query      query
   * @param baseRunner baseRunner
   * @return QueryRunner
   */
  public QueryRunner<T> makeStringSerdeQueryRunner(final ObjectMapper mapper, final QueryToolChest<T, Query<T>> toolChest, final Query<T> query, final QueryRunner<T> baseRunner)
  {
    return (queryPlus, map) -> {
      try {
        Sequence<T> resultSeq = baseRunner.run(queryPlus, ResponseContext.createEmpty());
        final Yielder<Object> yielder = resultSeq.toYielder(null, new YieldingAccumulator<Object, T>()
        {
          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        });
        String resultStr = mapper.writer().writeValueAsString(yielder);

        List<T> resultRows = Lists.transform(
            readQueryResultArrayFromString(resultStr),
            toolChest.makePreComputeManipulatorFn(queryPlus.getQuery(),
                MetricManipulatorFns.deserializing()));
        return Sequences.simple(resultRows);
      } 
      catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    };
  }

  /**
   * Format the result array.
   *
   * @param str str
   * @return List
   * @throws Exception Exception
   */
  private List<T> readQueryResultArrayFromString(String str) throws Exception
  {
    List<T> result = new ArrayList<>();

    JsonParser jp = mapper.getFactory().createParser(str);

    if (jp.nextToken() != JsonToken.START_ARRAY) {
      throw new IAE("not an array [%s]", str);
    }

    ObjectCodec objectCodec = jp.getCodec();

    while (jp.nextToken() != JsonToken.END_ARRAY) {
      result.add(objectCodec.readValue(jp, toolChest.getResultTypeReference()));
    }
    return result;
  }

  /**
   * get method.
   *
   * @return ObjectMapper
   */
  public ObjectMapper getObjectMapper()
  {
    return mapper;
  }
}
