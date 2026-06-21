/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryAlertEventStore implements AlertEventStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryAlertEventStore.class);

    private final ConcurrentSkipListMap<Long, List<AlertEvent>> eventsByTime;
    private final Map<String, List<AlertEvent>> eventsByRuleId;
    private final Map<String, List<AlertEvent>> eventsByFingerprint;
    private final AtomicLong eventCount;

    public InMemoryAlertEventStore() {
        this.eventsByTime = new ConcurrentSkipListMap<>();
        this.eventsByRuleId = new ConcurrentHashMap<>();
        this.eventsByFingerprint = new ConcurrentHashMap<>();
        this.eventCount = new AtomicLong(0);
    }

    @Override
    public void saveAlertEvent(AlertEvent event) {
        if (event == null) {
            return;
        }

        long timestamp = event.getTimestamp();
        eventsByTime.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(event);

        if (event.getRuleId() != null) {
            eventsByRuleId.computeIfAbsent(event.getRuleId(), k -> new ArrayList<>()).add(event);
        }

        if (event.getFingerprint() != null) {
            eventsByFingerprint.computeIfAbsent(event.getFingerprint(), k -> new ArrayList<>()).add(event);
        }

        eventCount.incrementAndGet();
        logger.debug("Saved alert event: type={}, ruleId={}, severity={}",
                event.getAlertType(), event.getRuleId(), event.getSeverity());
    }

    @Override
    public List<AlertEvent> getAlertEventsByTimeRange(long startTime, long endTime) {
        List<AlertEvent> result = new ArrayList<>();
        for (Map.Entry<Long, List<AlertEvent>> entry :
                eventsByTime.subMap(startTime, true, endTime, true).entrySet()) {
            result.addAll(entry.getValue());
        }
        result.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        logger.debug("Retrieved {} alert events in time range [{}, {}]", result.size(), startTime, endTime);
        return result;
    }

    @Override
    public List<AlertEvent> getAlertEventsByRuleId(String ruleId) {
        List<AlertEvent> events = eventsByRuleId.getOrDefault(ruleId, new ArrayList<>());
        events.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        return events;
    }

    @Override
    public List<AlertEvent> getAlertEventsByFingerprint(String fingerprint) {
        List<AlertEvent> events = eventsByFingerprint.getOrDefault(fingerprint, new ArrayList<>());
        events.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        return events;
    }

    @Override
    public List<AlertEvent> getAlertEventsByRuleAndTimeRange(String ruleId, long startTime, long endTime) {
        List<AlertEvent> timeRangeEvents = getAlertEventsByTimeRange(startTime, endTime);
        List<AlertEvent> result = new ArrayList<>();
        for (AlertEvent event : timeRangeEvents) {
            if (ruleId.equals(event.getRuleId())) {
                result.add(event);
            }
        }
        return result;
    }

    @Override
    public List<AlertEvent> getAllAlertEvents() {
        List<AlertEvent> result = new ArrayList<>();
        for (List<AlertEvent> events : eventsByTime.values()) {
            result.addAll(events);
        }
        result.sort((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()));
        return result;
    }

    @Override
    public void clearAlertEvents() {
        eventsByTime.clear();
        eventsByRuleId.clear();
        eventsByFingerprint.clear();
        eventCount.set(0);
        logger.info("Cleared all alert events from store");
    }

    @Override
    public long getAlertEventCount() {
        return eventCount.get();
    }

    @Override
    public void close() {
        clearAlertEvents();
        logger.info("Closed alert event store");
    }
}
