/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTimeZone;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DateMathParserTests extends ElasticsearchTestCase {
    FormatDateTimeFormatter formatter = Joda.forPattern("dateOptionalTime");
    DateMathParser parser = new DateMathParser(formatter, TimeUnit.MILLISECONDS);

    private static Callable<Long> callable(final long value) {
        return new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return value;
            }
        };
    }

    void assertDateMathEquals(String toTest, String expected) {
        assertDateMathEquals(toTest, expected, 0, false, null);
    }

    void assertDateMathEquals(String toTest, String expected, long now, boolean roundUp, DateTimeZone timeZone) {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        long gotMillis = parser.parse(toTest, callable(now), roundUp, timeZone);
        long expectedMillis = parser.parse(expected, callable(0));
        if (gotMillis != expectedMillis) {
            fail("Date math not equal\n" + "Original              : " + toTest + "\n" + "Parsed                : " + formatter.printer().print(gotMillis) + "\n" + "Expected              : " + expected + "\n" + "Expected milliseconds : " + expectedMillis + "\n" + "Actual milliseconds   : " + gotMillis + "\n");
        }
    }

    public void testBasicDates() {
        assertDateMathEquals("2014", "2014-01-01T00:00:00.000");
        assertDateMathEquals("2014-05", "2014-05-01T00:00:00.000");
        assertDateMathEquals("2014-05-30", "2014-05-30T00:00:00.000");
        assertDateMathEquals("2014-05-30T20", "2014-05-30T20:00:00.000");
        assertDateMathEquals("2014-05-30T20:21", "2014-05-30T20:21:00.000");
        assertDateMathEquals("2014-05-30T20:21:35", "2014-05-30T20:21:35.000");
        assertDateMathEquals("2014-05-30T20:21:35.123", "2014-05-30T20:21:35.123");
    }

    public void testBasicMath() {
        assertDateMathEquals("2014-11-18||+y", "2015-11-18");
        assertDateMathEquals("2014-11-18||-2y", "2012-11-18");

        assertDateMathEquals("2014-11-18||+3M", "2015-02-18");
        assertDateMathEquals("2014-11-18||-M", "2014-10-18");

        assertDateMathEquals("2014-11-18||+1w", "2014-11-25");
        assertDateMathEquals("2014-11-18||-3w", "2014-10-28");

        assertDateMathEquals("2014-11-18||+22d", "2014-12-10");
        assertDateMathEquals("2014-11-18||-423d", "2013-09-21");

        assertDateMathEquals("2014-11-18T14||+13h", "2014-11-19T03");
        assertDateMathEquals("2014-11-18T14||-1h", "2014-11-18T13");
        assertDateMathEquals("2014-11-18T14||+13H", "2014-11-19T03");
        assertDateMathEquals("2014-11-18T14||-1H", "2014-11-18T13");

        assertDateMathEquals("2014-11-18T14:27||+10240m", "2014-11-25T17:07");
        assertDateMathEquals("2014-11-18T14:27||-10m", "2014-11-18T14:17");

        assertDateMathEquals("2014-11-18T14:27:32||+60s", "2014-11-18T14:28:32");
        assertDateMathEquals("2014-11-18T14:27:32||-3600s", "2014-11-18T13:27:32");
    }

    public void testMultipleAdjustments() {
        assertDateMathEquals("2014-11-18||+1M-1M", "2014-11-18");
        assertDateMathEquals("2014-11-18||+1M-1m", "2014-12-17T23:59");
        assertDateMathEquals("2014-11-18||-1m+1M", "2014-12-17T23:59");
        assertDateMathEquals("2014-11-18||+1M/M", "2014-12-01");
        assertDateMathEquals("2014-11-18||+1M/M+1h", "2014-12-01T01");
    }

    public void testNow() {
        final long now = parser.parse("2014-11-18T14:27:32", callable(0), false, null);

        assertDateMathEquals("now", "2014-11-18T14:27:32", now, false, null);
        assertDateMathEquals("now+M", "2014-12-18T14:27:32", now, false, null);
        assertDateMathEquals("now-2d", "2014-11-16T14:27:32", now, false, null);
        assertDateMathEquals("now/m", "2014-11-18T14:27", now, false, null);
    }

    public void testRounding() {
        assertDateMathEquals("2014-11-18||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/y", "2014-12-31T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014||/y", "2014-12-31T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-01-01T00:00:00.001||/y", "2014-12-31T23:59:59.999", 0, true, null);
        // rounding should also take into account time zone
        assertDateMathEquals("2014-11-18||/y", "2013-12-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/y", "2014-12-31T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/M", "2014-11-30T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11||/M", "2014-11-30T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/M", "2014-10-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/M", "2014-11-30T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/w", "2014-11-23T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-23T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("+01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-17T01:00:00.000Z", 0, false, DateTimeZone.forID("-01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-23T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-07-22||/w", "2014-07-20T22:00:00.000Z", 0, false, DateTimeZone.forID("CET")); // with DST

        assertDateMathEquals("2014-11-18T14||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/d", "2014-11-18T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-18T23:59:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14:59:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:27:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:27:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:32.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:32.999", 0, true, null);
    }

    void assertParseException(String msg, String date) {
        try {
            parser.parse(date, callable(0));
            fail("Date: " + date + "\n" + msg);
        } catch (ElasticsearchParseException e) {
            // expected
        }
    }

    public void testIllegalMathFormat() {
        assertParseException("Expected date math unsupported operator exception", "2014-11-18||*5");
        assertParseException("Expected date math incompatible rounding exception", "2014-11-18||/2m");
        assertParseException("Expected date math illegal unit type exception", "2014-11-18||+2a");
        assertParseException("Expected date math truncation exception", "2014-11-18||+12");
        assertParseException("Expected date math truncation exception", "2014-11-18||-");
    }

    public void testOnlyCallsNowIfNecessary() {
        final AtomicBoolean called = new AtomicBoolean();
        final Callable<Long> now = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                called.set(true);
                return 42L;
            }
        };
        parser.parse("2014-11-18T14:27:32", now, false, null);
        assertFalse(called.get());
        parser.parse("now/d", now, false, null);
        assertTrue(called.get());
    }
}
