/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV2;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.text.StringSubstitutor;

/**
 * Parses JSON payloads using Google's JSON API model library, emitting a BigQuery-specific
 * TableRow.
 *
 * <p>We also perform some light manipulation of the parsed JSON to match details of our table
 * schemas in BigQuery. In particular, we pull out submission_timestamp to a top level field so we
 * can use it as our partitioning field.
 */
public class PubsubMessageToTableRow
    extends MapElementsWithErrors<PubsubMessage, KV<TableDestination, TableRow>> {

  public static PubsubMessageToTableRow of(ValueProvider<String> tableSpecTemplate) {
    return new PubsubMessageToTableRow(tableSpecTemplate);
  }

  public static final String SUBMISSION_TIMESTAMP = "submission_timestamp";
  public static final TimePartitioning TIME_PARTITIONING = new TimePartitioning()
      .setField(SUBMISSION_TIMESTAMP);

  private final ValueProvider<String> tableSpecTemplate;

  // We'll instantiate these on first use.
  private transient Cache<DatasetReference, Set<String>> tableListingCache;
  private transient Cache<TableReference, List<List<String>>> tableMapFieldPathCache;

  private PubsubMessageToTableRow(ValueProvider<String> tableSpecTemplate) {
    this.tableSpecTemplate = tableSpecTemplate;
  }

  @Override
  protected KV<TableDestination, TableRow> processElement(PubsubMessage message)
      throws IOException {
    message = PubsubConstraints.ensureNonNull(message);
    // Only letters, numbers, and underscores are allowed in BigQuery dataset and table names,
    // but some doc types and namespaces contain '-', so we convert to '_'.
    final Map<String, String> attributes = Maps.transformValues(message.getAttributeMap(),
        v -> v.replaceAll("-", "_"));
    final String tableSpec = StringSubstitutor.replace(tableSpecTemplate.get(), attributes);

    // Send to error collection if incomplete tableSpec; $ is not a valid char in tableSpecs.
    if (tableSpec.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured BigQuery output template: "
          + tableSpecTemplate.get());
    }

    final TableDestination tableDestination = new TableDestination(tableSpec, null,
        TIME_PARTITIONING);
    final TableReference ref = BigQueryHelpers.parseTableSpec(tableSpec);
    final DatasetReference datasetRef = new DatasetReference().setProjectId(ref.getProjectId())
        .setDatasetId(ref.getDatasetId());

    // Get and cache a listing of table names for this dataset.
    Set<String> tablesInDataset;
    if (tableListingCache == null) {
      tableListingCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).build();
    }
    try {
      tablesInDataset = tableListingCache.get(datasetRef, () -> {
        Set<String> tableSet = new HashSet<>();
        BigQuery service = BigQueryOptions.newBuilder().setProjectId(ref.getProjectId()).build()
            .getService();
        Dataset dataset = service.getDataset(ref.getDatasetId());
        if (dataset != null) {
          dataset.list().iterateAll().forEach(t -> {
            tableSet.add(t.getTableId().getTable());
          });
        }
        return tableSet;
      });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }

    // Send to error collection if dataset or table doesn't exist so BigQueryIO doesn't throw a
    // pipeline execution exception.
    if (tablesInDataset.isEmpty()) {
      throw new IllegalArgumentException("Resolved destination dataset does not exist or has no "
          + " tables for tableSpec " + tableSpec);
    } else if (!tablesInDataset.contains(ref.getTableId())) {
      throw new IllegalArgumentException("Resolved destination table does not exist: " + tableSpec);
    }

    // Get and cache a list of paths to map-type fields in this schema.
    List<List<String>> mapFieldPaths;
    if (tableMapFieldPathCache == null) {
      tableMapFieldPathCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1))
          .build();
    }
    try {
      mapFieldPaths = tableMapFieldPathCache.get(ref, () -> {
        List<List<String>> paths = new ArrayList<>();
        BigQuery service = BigQueryOptions.newBuilder().setProjectId(ref.getProjectId()).build()
            .getService();
        Table table = service.getTable(ref.getDatasetId(), ref.getTableId());
        if (table != null) {
          accumulateMapFieldPaths(ImmutableList.of(), table.getDefinition().getSchema().getFields(),
              paths);
        }
        return paths;
      });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }

    TableRow tableRow = buildTableRow(message.getPayload());
    transformMapFields(tableRow, mapFieldPaths);
    return KV.of(tableDestination, tableRow);
  }

  @Override
  public WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> expand(
      PCollection<PubsubMessage> input) {
    WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> result = super.expand(input);
    result.output().setCoder(KvCoder.of(TableDestinationCoderV2.of(), TableRowJsonCoder.of()));
    return result;
  }

  @VisibleForTesting
  static TableRow buildTableRow(byte[] payload) throws IOException {
    TableRow tableRow = Json.readTableRow(payload);
    promoteFields(tableRow);
    return tableRow;
  }

  /**
   * BigQuery cannot partition or cluster tables by a nested field, so we promote some important
   * attributes out of metadata to top-level fields.
   */
  private static void promoteFields(TableRow tableRow) {
    Optional<Map> metadata = Optional.ofNullable(tableRow).map(row -> tableRow.get("metadata"))
        .filter(Map.class::isInstance).map(Map.class::cast);
    if (metadata.isPresent()) {
      Object submissionTimestamp = metadata.get().remove("submission_timestamp");
      if (submissionTimestamp instanceof String) {
        tableRow.putIfAbsent("submission_timestamp", submissionTimestamp);
      }
      Object sampleId = metadata.get().remove("sample_id");
      if (sampleId instanceof String) {
        try {
          Long asLong = Long.valueOf((String) sampleId);
          tableRow.putIfAbsent("sample_id", asLong);
        } catch (NumberFormatException ignore) {
          // pass
        }
      }
    }
  }

  /**
   * Recursively descend through a list of BQ schema fields, appending paths to discovered
   * map-type fields.
   *
   * @param position a list of parent field names leading up to the passed fields
   * @param fields all the fields of the current RECORD-type field
   * @param mapFieldPaths a growing list of paths to all map-type fields
   */
  public static void accumulateMapFieldPaths(List<String> position, FieldList fields,
      List<List<String>> mapFieldPaths) {
    fields.forEach(field -> {
      if (LegacySQLTypeName.RECORD.equals(field.getType())) {
        ArrayList<String> newPosition = new ArrayList<>(position);
        newPosition.add(field.getName());
        // Make sure we descend into nested maps first, so that we end up transforming
        // the deepest maps first.
        accumulateMapFieldPaths(newPosition, field.getSubFields(), mapFieldPaths);
        // If this RECORD in the BQ schema looks like a transformed map, we record the path.
        if (field.getSubFields().size() == 2 //
            && field.getSubFields().get(0).getName().equals("key") //
            && field.getSubFields().get(1).getName().equals("value")) {
          mapFieldPaths.add(newPosition);
        }
      }
    });
  }

  /**
   * Given a parsed json object and list of paths to map-type fields, descend through the
   * structure and replace all map-type fields with an equivalent list of key/value pairs.
   */
  public static void transformMapFields(TableRow row, List<List<String>> mapFieldPaths) {
    mapFieldPaths.forEach(path -> {
      List<String> parentPath = new ArrayList<>(path);
      String targetFieldName = parentPath.remove(path.size() - 1);
      // Since the path was determined from the BQ schema where maps were already transformed,
      // we have to remove elements named "value".
      parentPath = parentPath.stream().filter(n -> !"value".equals(n)).collect(Collectors.toList());
      Stream<Map<String, Object>> stream = ImmutableList.<Map<String, Object>>of(row).stream();
      for (String name : parentPath) {
        stream = stream.map(f -> f.get(name)).flatMap(f -> {
          if (f == null) {
            return ImmutableList.<Object>of().stream();
          } else if (f instanceof List) {
            return ((List<Object>) f).stream();
          } else {
            return ImmutableList.of((Object) f).stream();
          }
        }).filter(Map.class::isInstance).map(f -> (Map<String, Object>) f);
      }
      stream.forEach(parent -> {
        Object target = parent.get(targetFieldName);
        if (target instanceof Map) {
          List<ImmutableMap<String, Object>> modified = ((Map<String, Object>) target).entrySet()
              .stream()
              .map(entry -> ImmutableMap.of("key", entry.getKey(), "value", entry.getValue()))
              .collect(Collectors.toList());
          parent.put(targetFieldName, modified);
        }
      });
    });
  }

}
