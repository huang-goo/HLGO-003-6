/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEventStore implements EventStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryEventStore.class);

    private final ConcurrentSkipListMap<Long, List<AnomalyScoreEvent>> eventsByTime;
    private final Map<String, List<AnomalyScoreEvent>> eventsByDimension;
    private final AtomicLong eventCount;

    public InMemoryEventStore() {
        this.eventsByTime = new ConcurrentSkipListMap<>();
        this.eventsByDimension = new ConcurrentHashMap<>();
        this.eventCount = new AtomicLong(0);
    }

    @Override
    public void saveEvent(AnomalyScoreEvent event) {
        if (event == null) {
            return;
        }

        long timestamp = event.getTimestamp();
        eventsByTime.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(event);

        for (Map.Entry<String, String> entry : event.getDimensions().entrySet()) {
            String key = entry.getKey() + "=" + entry.getValue();
            eventsByDimension.computeIfAbsent(key, k -> new ArrayList<>()).add(event);
        }

        eventCount.incrementAndGet();
        logger.debug("Saved event: timestamp={}, stream6={}, stream9={}, stream11={}",
                timestamp, event.getStream6Score(), event.getStream9Score(), event.getStream11Score());
    }

    @Override
    public List<AnomalyScoreEvent> getEventsByTimeRange(long startTime, long endTime) {
        List<AnomalyScoreEvent> result = new ArrayList<>();
        for (Map.Entry<Long, List<AnomalyScoreEvent>> entry :
                eventsByTime.subMap(startTime, true, endTime, true).entrySet()) {
            result.addAll(entry.getValue());
        }
        result.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        logger.debug("Retrieved {} events in time range [{}, {}]", result.size(), startTime, endTime);
        return result;
    }

    @Override
    public List<AnomalyScoreEvent> getEventsByTimeRangeAndDimensions(
            long startTime, long endTime, Map<String, String> dimensionFilters) {
        List<AnomalyScoreEvent> timeRangeEvents = getEventsByTimeRange(startTime, endTime);

        if (dimensionFilters == null || dimensionFilters.isEmpty()) {
            return timeRangeEvents;
        }

        List<AnomalyScoreEvent> result = new ArrayList<>();
        for (AnomalyScoreEvent event : timeRangeEvents) {
            boolean matches = true;
            for (Map.Entry<String, String> filter : dimensionFilters.entrySet()) {
                String actualValue = event.getDimensions().get(filter.getKey());
                if (actualValue == null || !actualValue.equals(filter.getValue())) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                result.add(event);
            }
        }
        logger.debug("Retrieved {} events in time range with dimension filters", result.size());
        return result;
    }

    @Override
    public List<AnomalyScoreEvent> getAllEvents() {
        List<AnomalyScoreEvent> result = new ArrayList<>();
        for (List<AnomalyScoreEvent> events : eventsByTime.values()) {
            result.addAll(events);
        }
        result.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        return result;
    }

    @Override
    public void clearEvents() {
        eventsByTime.clear();
        eventsByDimension.clear();
        eventCount.set(0);
        logger.info("Cleared all events from store");
    }

    @Override
    public long getEventCount() {
        return eventCount.get();
    }

    @Override
    public void close() {
        clearEvents();
        logger.info("Closed event store");
    }
}
