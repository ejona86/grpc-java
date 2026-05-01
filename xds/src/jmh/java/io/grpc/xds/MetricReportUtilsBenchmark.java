/*
 * Copyright 2026 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds.internal;

import com.google.common.collect.ImmutableMap;
import io.grpc.services.InternalCallMetricRecorder;
import io.grpc.services.MetricReport;
import java.util.Arrays;
import java.util.OptionalDouble;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(time=1)
@Measurement(time=1)
@State(Scope.Thread)
public class MetricReportUtilsBenchmark {
  List<MetricReport> reports = Arrays.asList(
      InternalCallMetricRecorder.createMetricReport(
          1, 2, 3, 4, 5, ImmutableMap.of(),
          ImmutableMap.of(),
          ImmutableMap.of()),
      InternalCallMetricRecorder.createMetricReport(
          0, 0, 0, 0, 0, ImmutableMap.of(),
          ImmutableMap.of("stuff", 1.0, "that", 2.0, "is", 3.0, "never", 4.0, "found", 5.0),
          ImmutableMap.of("stuff", 1.0, "that", 2.0, "is", 3.0, "never", 4.0, "found", 5.0)),
      InternalCallMetricRecorder.createMetricReport(
          0, 0, 0, 0, 0, ImmutableMap.of(),
          ImmutableMap.of("foo", 2.0, "bar", 3.0, "ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory", 4.0),
          ImmutableMap.of("foo", 2.0, "bar", 3.0, "ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory", 4.0))
      );
  List<String> metricNames = Arrays.asList(
      "cpu_utilization",
      "mem_utilization",
      "utilization.foo",
      "utilization.bar",
      "utilization.ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory",
      "named_metrics.foo",
      "named_metrics.bar",
      "named_metrics.ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory");
  static final class Struct {
    final String name1;
    final String name2;

    public Struct(String name1) {
      this(name1, null);
    }

    public Struct(String name1, String name2) {
      this.name1 = name1;
      this.name2 = name2;
    }
  }
  List<Struct> metricNames2 = Arrays.asList(
      new Struct("cpu_utilization"),
      new Struct("mem_utilization"),
      new Struct("utilization", "foo"),
      new Struct("utilization", "bar"),
      new Struct("utilization", "ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory"),
      new Struct("named_metrics", "foo"),
      new Struct("named_metrics", "bar"),
      new Struct("named_metrics", "ThisWouldMakeAGreatJavaClassObjectProviderSupplierFactory"));

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public double getMetricNamesOldSkool() throws Exception {
    double sum = 0;
    for (int i = 0; i < reports.size(); i++) {
      MetricReport report = reports.get(i);
      for (int j = 0; j < metricNames.size(); j++) {
        String name = metricNames.get(j);
        OptionalDouble result = MetricReportUtils.getMetric(report, name);
        if (result.isPresent()) {
          double d = result.getAsDouble();
          if (!Double.isNaN(d) && d > 0) {
            sum += result.getAsDouble();
            break;
          }
        }
      }
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public double getMetricNamesManual() throws Exception {
    double sum = 0;
    for (MetricReport report : reports) {
      for (String name : metricNames) {
        OptionalDouble result = MetricReportUtils.getMetric(report, name);
        if (result.isPresent()) {
          double d = result.getAsDouble();
          if (!Double.isNaN(d) && d > 0) {
            sum += result.getAsDouble();
            break;
          }
        }
      }
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public double getMetricNamesStream() throws Exception {
    double sum = 0;
    for (MetricReport report : reports) {
      OptionalDouble result = metricNames.stream()
          .map(name -> MetricReportUtils.getMetric(report, name))
          .filter(OptionalDouble::isPresent)
          .mapToDouble(OptionalDouble::getAsDouble)
          .filter(d -> !Double.isNaN(d) && d > 0)
          .max();
        if (result.isPresent()) {
          sum += result.getAsDouble();
        }
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public double getMetricNames2Manual() throws Exception {
    double sum = 0;
    for (MetricReport report : reports) {
      for (Struct name : metricNames2) {
        OptionalDouble result = MetricReportUtils.getMetric2(report, name.name1, name.name2);
        if (result.isPresent()) {
          double d = result.getAsDouble();
          if (!Double.isNaN(d) && d > 0) {
            sum += result.getAsDouble();
            break;
          }
        }
      }
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public double getMetricNames2Stream() throws Exception {
    double sum = 0;
    for (MetricReport report : reports) {
      OptionalDouble result = metricNames2.stream()
          .map(name -> MetricReportUtils.getMetric2(report, name.name1, name.name2))
          .filter(OptionalDouble::isPresent)
          .mapToDouble(OptionalDouble::getAsDouble)
          .filter(d -> !Double.isNaN(d) && d > 0)
          .max();
        if (result.isPresent()) {
          sum += result.getAsDouble();
        }
    }
    return sum;
  }
}
