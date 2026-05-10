package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.storage.SpanStatistics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ClickHouseResultMapperStatisticsTest {

  private BinaryStreamReader.ArrayValue mockAnnotations;

  @BeforeEach
  void setUp() {
    mockAnnotations = mock(BinaryStreamReader.ArrayValue.class);
    when(mockAnnotations.asList()).thenReturn(List.of());
  }

  private Map<String, Object> baseRecord() {
    Map<String, Object> record = new HashMap<>();
    record.put("trace_id", BigInteger.ONE);
    record.put("annotations", mockAnnotations);
    return record;
  }

  @Test
  void statisticsJoinFragment_containsDatabaseName() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("mydb");
    assertTrue(fragment.contains("mydb.spans_aggregate_stats"));
  }

  @Test
  void statisticsJoinFragment_startsWithLeftJoin() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.startsWith(" LEFT JOIN"));
  }

  @Test
  void statisticsJoinFragment_containsAllDurationMergeFunctions() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.contains("medianMerge(median_duration)"));
    assertTrue(fragment.contains("avgMerge(average_duration)"));
    assertTrue(fragment.contains("quantileMerge(p50)"));
    assertTrue(fragment.contains("quantileMerge(p95)"));
    assertTrue(fragment.contains("quantileMerge(p99)"));
  }

  @Test
  void statisticsJoinFragment_containsCountMergeFunctions() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.contains("sumMerge(success_count)"));
    assertTrue(fragment.contains("sumMerge(error_count)"));
    assertTrue(fragment.contains("sumMerge(total_count)"));
  }

  @Test
  void statisticsJoinFragment_containsGroupByClause() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.contains("GROUP BY span_name, span_kind, service_name"));
  }

  @Test
  void statisticsJoinFragment_containsThreeColumnJoinCondition() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.contains("s.name = stats.span_name"));
    assertTrue(fragment.contains("s.kind = stats.span_kind"));
    assertTrue(fragment.contains("s.local_endpoint_service_name = stats.service_name"));
  }

  @Test
  void statisticsJoinFragment_aliasedAsStats() {
    String fragment = ClickHouseResultMapper.getStatisticsJoinFragment("zipkin");
    assertTrue(fragment.contains("AS stats"));
  }

  @Test
  void toSpan_allStatisticsPresent_createsSpanStatisticsWithCorrectValues() {
    Map<String, Object> record = baseRecord();
    record.put("name", "checkout");
    record.put("kind", "SERVER");
    record.put("median_duration", new BigDecimal("100.5"));
    record.put("average_duration", new BigDecimal("110.0"));
    record.put("p50", new BigDecimal("90.0"));
    record.put("p95", new BigDecimal("200.0"));
    record.put("p99", new BigDecimal("300.0"));
    record.put("success_count", 50L);
    record.put("error_count", 5L);
    record.put("total_count", 55L);

    SpanStatistics stats = ClickHouseResultMapper.toSpan(record).statistics();

    assertNotNull(stats);
    assertEquals(new BigDecimal("100.5"), stats.medianDuration);
    assertEquals(new BigDecimal("110.0"), stats.averageDuration);
    assertEquals(new BigDecimal("90.0"), stats.p50);
    assertEquals(new BigDecimal("200.0"), stats.p95);
    assertEquals(new BigDecimal("300.0"), stats.p99);
    assertEquals(50L, stats.successCount);
    assertEquals(5L, stats.errorCount);
    assertEquals(55L, stats.totalCount);
  }

  @Test
  void toSpan_allStatisticsNull_statisticsNotAttached() {
    Map<String, Object> record = baseRecord();
    assertNull(ClickHouseResultMapper.toSpan(record).statistics());
  }

  @Test
  void toSpan_onlyOneStatFieldNonNull_statisticsAttachedWithZeroDefaults() {
    Map<String, Object> record = baseRecord();
    record.put("success_count", 1L);

    SpanStatistics stats = ClickHouseResultMapper.toSpan(record).statistics();

    assertNotNull(stats);
    assertEquals(1L, stats.successCount);
    assertEquals(0L, stats.errorCount);
    assertEquals(0L, stats.totalCount);
    assertEquals(BigDecimal.ZERO, stats.medianDuration);
    assertEquals(BigDecimal.ZERO, stats.averageDuration);
    assertEquals(BigDecimal.ZERO, stats.p50);
    assertEquals(BigDecimal.ZERO, stats.p95);
    assertEquals(BigDecimal.ZERO, stats.p99);
  }

  @Test
  void toSpan_statisticsUsesSpanNameFromRecord() {
    Map<String, Object> record = baseRecord();
    record.put("name", "checkout");
    record.put("median_duration", new BigDecimal("50.0"));

    assertEquals("checkout", ClickHouseResultMapper.toSpan(record).statistics().spanName);
  }

  @Test
  void toSpan_statisticsUsesKindFromRecord() {
    Map<String, Object> record = baseRecord();
    record.put("kind", "SERVER");
    record.put("median_duration", new BigDecimal("50.0"));

    assertEquals("SERVER", ClickHouseResultMapper.toSpan(record).statistics().spanKind);
  }

  @Test
  void toSpan_statisticsUsesEmptyStringForNullKind() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", new BigDecimal("50.0"));

    assertEquals("", ClickHouseResultMapper.toSpan(record).statistics().spanKind);
  }

  @Test
  void toSpan_bigDecimalStatField_passedThrough() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", new BigDecimal("123.456"));

    assertEquals(new BigDecimal("123.456"), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_doubleStatField_convertedViaBigDecimalValueOf() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", 99.5d);

    assertEquals(BigDecimal.valueOf(99.5), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_floatStatField_converted() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", 50.0f);

    assertNotNull(ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_longStatField_convertedToBigDecimal() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", 1000L);

    assertEquals(BigDecimal.valueOf(1000L), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_integerStatField_convertedToBigDecimal() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", 42);

    assertEquals(BigDecimal.valueOf(42), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_bigIntegerStatField_convertedToBigDecimal() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", new BigInteger("9999"));

    assertEquals(new BigDecimal(new BigInteger("9999")), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_stringStatField_parsedAsBigDecimal() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", "250.75");

    assertEquals(new BigDecimal("250.75"), ClickHouseResultMapper.toSpan(record).statistics().medianDuration);
  }

  @Test
  void toSpan_unparsableStringStatField_treatedAsNull() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", "not-a-number");

    assertNull(ClickHouseResultMapper.toSpan(record).statistics());
  }

  @Test
  void toSpan_unknownTypeStatField_treatedAsNull() {
    Map<String, Object> record = baseRecord();
    record.put("median_duration", new Object());

    assertNull(ClickHouseResultMapper.toSpan(record).statistics());
  }

  @Test
  void toSpan_longCountField_passedDirectly() {
    Map<String, Object> record = baseRecord();
    record.put("success_count", 50L);

    assertEquals(50L, ClickHouseResultMapper.toSpan(record).statistics().successCount);
  }

  @Test
  void toSpan_integerCountField_widened() {
    Map<String, Object> record = baseRecord();
    record.put("error_count", 10);

    assertEquals(10L, ClickHouseResultMapper.toSpan(record).statistics().errorCount);
  }

  @Test
  void toSpan_bigIntegerCountField_narrowed() {
    Map<String, Object> record = baseRecord();
    record.put("total_count", new BigInteger("100"));

    assertEquals(100L, ClickHouseResultMapper.toSpan(record).statistics().totalCount);
  }

  @Test
  void toSpan_stringCountField_parsed() {
    Map<String, Object> record = baseRecord();
    record.put("total_count", "75");

    assertEquals(75L, ClickHouseResultMapper.toSpan(record).statistics().totalCount);
  }

  @Test
  void toSpan_unparsableStringCountField_treatedAsNull() {
    Map<String, Object> record = baseRecord();
    record.put("total_count", "abc");

    assertNull(ClickHouseResultMapper.toSpan(record).statistics());
  }

  @Test
  void toSpan_zonedDateTimeCountField_convertedToMicros() {
    Map<String, Object> record = baseRecord();
    Instant instant = Instant.ofEpochSecond(1000L, 500_000L);
    ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    record.put("success_count", zdt);

    long expected = 1000L * 1_000_000L + 500_000L / 1_000L;
    assertEquals(expected, ClickHouseResultMapper.toSpan(record).statistics().successCount);
  }

  @Test
  void groupSpansByTraceId_emptyList_returnsEmptyList() {
    assertTrue(ClickHouseResultMapper.groupSpansByTraceId(List.of()).isEmpty());
  }

  @Test
  void groupSpansByTraceId_singleSpan_returnsSingleGroup() {
    Span span = Span.newBuilder().traceId("abc1").id("1").build();
    List<List<Span>> result = ClickHouseResultMapper.groupSpansByTraceId(List.of(span));
    assertEquals(1, result.size());
    assertEquals(1, result.get(0).size());
  }

  @Test
  void groupSpansByTraceId_spansWithSameTraceId_groupedTogether() {
    Span s1 = Span.newBuilder().traceId("aaaa").id("1").build();
    Span s2 = Span.newBuilder().traceId("aaaa").id("2").build();
    Span s3 = Span.newBuilder().traceId("aaaa").id("3").build();
    List<List<Span>> result = ClickHouseResultMapper.groupSpansByTraceId(List.of(s1, s2, s3));
    assertEquals(1, result.size());
    assertEquals(3, result.get(0).size());
  }

  @Test
  void groupSpansByTraceId_spansWithDistinctTraceIds_eachInOwnGroup() {
    Span s1 = Span.newBuilder().traceId("aaaa").id("1").build();
    Span s2 = Span.newBuilder().traceId("bbbb").id("2").build();
    Span s3 = Span.newBuilder().traceId("cccc").id("3").build();
    List<List<Span>> result = ClickHouseResultMapper.groupSpansByTraceId(List.of(s1, s2, s3));
    assertEquals(3, result.size());
    result.forEach(group -> assertEquals(1, group.size()));
  }

  @Test
  void groupSpansByTraceId_mixedTraceIds_groupsMatchTraceIds() {
    Span s1 = Span.newBuilder().traceId("aaaa").id("1").build();
    Span s2 = Span.newBuilder().traceId("aaaa").id("2").build();
    Span s3 = Span.newBuilder().traceId("bbbb").id("3").build();
    Span s4 = Span.newBuilder().traceId("cccc").id("4").build();
    List<List<Span>> result = ClickHouseResultMapper.groupSpansByTraceId(List.of(s1, s2, s3, s4));
    assertEquals(3, result.size());
    assertEquals(1, result.stream().filter(g -> g.size() == 2).count());
    assertEquals(2, result.stream().filter(g -> g.size() == 1).count());
  }
}
