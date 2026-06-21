/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;

import java.util.List;

public interface EventStore {
    void saveEvent(AnomalyScoreEvent event);

    List<AnomalyScoreEvent> getEventsByTimeRange(long startTime, long endTime);

    List<AnomalyScoreEvent> getEventsByTimeRangeAndDimensions(
            long startTime, long endTime, java.util.Map<String, String> dimensionFilters);

    List<AnomalyScoreEvent> getAllEvents();

    void clearEvents();

    long getEventCount();

    void close();
}
