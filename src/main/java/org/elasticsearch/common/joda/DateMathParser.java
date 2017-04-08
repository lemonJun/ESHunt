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
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 */
public class DateMathParser {

    private final FormatDateTimeFormatter dateTimeFormatter;

    private final TimeUnit timeUnit;

    public DateMathParser(FormatDateTimeFormatter dateTimeFormatter, TimeUnit timeUnit) {
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeUnit = timeUnit;
    }

    public long parse(String text, Callable<Long> now) {
        return parse(text, now, false, null);
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the query cache does not cache queries that make
    // use of `now`.
    public long parse(String text, Callable<Long> now, boolean roundCeil, DateTimeZone timeZone) {
        long time;
        String mathString;
        if (text.startsWith("now")) {
            try {
                time = now.call();
            } catch (Exception e) {
                throw new ElasticsearchParseException("Could not read the current timestamp", e);
            }
            mathString = text.substring("now".length());
        } else {
            int index = text.indexOf("||");
            String parseString;
            if (index == -1) {
                parseString = text;
                mathString = ""; // nothing else
            } else {
                parseString = text.substring(0, index);
                mathString = text.substring(index + 2);
            }
            if (roundCeil) {
                time = parseRoundCeilStringValue(parseString, timeZone);
            } else {
                time = parseStringValue(parseString, timeZone);
            }
        }

        if (mathString.isEmpty()) {
            return time;
        }
        return parseMath(mathString, time, roundCeil, timeZone);
    }

    private long parseMath(String mathString, long time, boolean roundUp, DateTimeZone timeZone) throws ElasticsearchParseException {
        if (timeZone == null) {
            timeZone = DateTimeZone.UTC;
        }
        MutableDateTime dateTime = new MutableDateTime(time, timeZone);
        for (int i = 0; i < mathString.length();) {
            char c = mathString.charAt(i++);
            final boolean round;
            final int sign;
            if (c == '/') {
                round = true;
                sign = 1;
            } else {
                round = false;
                if (c == '+') {
                    sign = 1;
                } else if (c == '-') {
                    sign = -1;
                } else {
                    throw new ElasticsearchParseException("operator not supported for date math [" + mathString + "]");
                }
            }

            if (i >= mathString.length()) {
                throw new ElasticsearchParseException("truncated date math [" + mathString + "]");
            }

            final int num;
            if (!Character.isDigit(mathString.charAt(i))) {
                num = 1;
            } else {
                int numFrom = i;
                while (i < mathString.length() && Character.isDigit(mathString.charAt(i))) {
                    i++;
                }
                if (i >= mathString.length()) {
                    throw new ElasticsearchParseException("truncated date math [" + mathString + "]");
                }
                num = Integer.parseInt(mathString.substring(numFrom, i));
            }
            if (round) {
                if (num != 1) {
                    throw new ElasticsearchParseException("rounding `/` can only be used on single unit types [" + mathString + "]");
                }
            }
            char unit = mathString.charAt(i++);
            MutableDateTime.Property propertyToRound = null;
            switch (unit) {
                case 'y':
                    if (round) {
                        propertyToRound = dateTime.yearOfCentury();
                    } else {
                        dateTime.addYears(sign * num);
                    }
                    break;
                case 'M':
                    if (round) {
                        propertyToRound = dateTime.monthOfYear();
                    } else {
                        dateTime.addMonths(sign * num);
                    }
                    break;
                case 'w':
                    if (round) {
                        propertyToRound = dateTime.weekOfWeekyear();
                    } else {
                        dateTime.addWeeks(sign * num);
                    }
                    break;
                case 'd':
                    if (round) {
                        propertyToRound = dateTime.dayOfMonth();
                    } else {
                        dateTime.addDays(sign * num);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        propertyToRound = dateTime.hourOfDay();
                    } else {
                        dateTime.addHours(sign * num);
                    }
                    break;
                case 'm':
                    if (round) {
                        propertyToRound = dateTime.minuteOfHour();
                    } else {
                        dateTime.addMinutes(sign * num);
                    }
                    break;
                case 's':
                    if (round) {
                        propertyToRound = dateTime.secondOfMinute();
                    } else {
                        dateTime.addSeconds(sign * num);
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("unit [" + unit + "] not supported for date math [" + mathString + "]");
            }
            if (propertyToRound != null) {
                if (roundUp) {
                    propertyToRound.roundCeiling();
                    dateTime.addMillis(-1); // subtract 1 millisecond to get the largest inclusive value
                } else {
                    propertyToRound.roundFloor();
                }
            }
        }
        return dateTime.getMillis();
    }

    /**
     * Get a DateTimeFormatter parser applying timezone if any.
     */
    public static DateTimeFormatter getDateTimeFormatterParser(FormatDateTimeFormatter dateTimeFormatter, DateTimeZone timeZone) {
        if (dateTimeFormatter == null) {
            return null;
        }

        DateTimeFormatter parser = dateTimeFormatter.parser();
        if (timeZone != null) {
            parser = parser.withZone(timeZone);
        }
        return parser;
    }

    private long parseStringValue(String value, DateTimeZone timeZone) {
        try {
            DateTimeFormatter parser = getDateTimeFormatterParser(dateTimeFormatter, timeZone);
            return parser.parseMillis(value);
        } catch (RuntimeException e) {
            try {
                // When date is given as a numeric value, it's a date in ms since epoch
                // By definition, it's a UTC date.
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new ElasticsearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number", e);
            }
        }
    }

    private long parseRoundCeilStringValue(String value, DateTimeZone timeZone) {
        try {
            // we create a date time for inclusive upper range, we "include" by default the day level data
            // so something like 2011-01-01 will include the full first day of 2011.
            // we also use 1970-01-01 as the base for it so we can handle searches like 10:12:55 (just time)
            // since when we index those, the base is 1970-01-01
            MutableDateTime dateTime = new MutableDateTime(1970, 1, 1, 23, 59, 59, 999, DateTimeZone.UTC);
            DateTimeFormatter parser = getDateTimeFormatterParser(dateTimeFormatter, timeZone);
            int location = parser.parseInto(dateTime, value, 0);
            // if we parsed all the string value, we are good
            if (location == value.length()) {
                return dateTime.getMillis();
            }
            // if we did not manage to parse, or the year is really high year which is unreasonable
            // see if its a number
            if (location <= 0 || dateTime.getYear() > 5000) {
                try {
                    long time = Long.parseLong(value);
                    return timeUnit.toMillis(time);
                } catch (NumberFormatException e1) {
                    throw new ElasticsearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number");
                }
            }
            return dateTime.getMillis();
        } catch (RuntimeException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new ElasticsearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number", e);
            }
        }
    }

    public static DateTimeZone parseZone(String text) throws IOException {
        int index = text.indexOf(':');
        if (index != -1) {
            int beginIndex = text.charAt(0) == '+' ? 1 : 0;
            // format like -02:30
            return DateTimeZone.forOffsetHoursMinutes(Integer.parseInt(text.substring(beginIndex, index)), Integer.parseInt(text.substring(index + 1)));
        } else {
            // id, listed here: http://joda-time.sourceforge.net/timezones.html
            return DateTimeZone.forID(text);
        }
    }

}
