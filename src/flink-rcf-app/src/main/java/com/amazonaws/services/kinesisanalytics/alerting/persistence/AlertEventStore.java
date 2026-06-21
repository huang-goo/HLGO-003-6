/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;

import java.util.List;

public interface AlertEventStore {
    void saveAlertEvent(AlertEvent event);

    List<AlertEvent> getAlertEventsByTimeRange(long startTime, long endTime);

    List<AlertEvent> getAlertEventsByRuleId(String ruleId);

    List<AlertEvent> getAlertEventsByFingerprint(String fingerprint);

    List<AlertEvent> getAlertEventsByRuleAndTimeRange(String ruleId, long startTime, long endTime);

    List<AlertEvent> getAllAlertEvents();

    void clearAlertEvents();

    long getAlertEventCount();

    void close();
}
