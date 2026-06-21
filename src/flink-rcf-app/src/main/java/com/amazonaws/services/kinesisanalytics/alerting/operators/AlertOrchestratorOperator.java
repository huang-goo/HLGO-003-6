/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.operators;

import com.amazonaws.services.kinesisanalytics.alerting.AlertOrchestrator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertOrchestratorOperator extends ProcessFunction<List<TimestreamPoint>, AlertEvent> {
    private static final Logger logger = LoggerFactory.getLogger(AlertOrchestratorOperator.class);

    private transient AlertOrchestrator orchestrator;
    private final List<AlertRule> initialRules;
    private final Map<String, String> defaultDimensions;

    public AlertOrchestratorOperator() {
        this(new ArrayList<>(), new HashMap<>());
    }

    public AlertOrchestratorOperator(List<AlertRule> initialRules, Map<String, String> defaultDimensions) {
        this.initialRules = initialRules != null ? new ArrayList<>(initialRules) : new ArrayList<>();
        this.defaultDimensions = defaultDimensions != null ? new HashMap<>(defaultDimensions) : new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        orchestrator = new AlertOrchestrator();

        for (AlertRule rule : initialRules) {
            orchestrator.registerRule(rule);
        }

        logger.info("AlertOrchestratorOperator initialized with {} rules", initialRules.size());
    }

    @Override
    public void processElement(List<TimestreamPoint> timestreamPoints,
                               ProcessFunction<List<TimestreamPoint>, AlertEvent>.Context context,
                               Collector<AlertEvent> collector) throws Exception {
        if (timestreamPoints == null || timestreamPoints.isEmpty()) {
            return;
        }

        AnomalyScoreEvent event = extractAnomalyScoreEvent(timestreamPoints);
        if (event == null) {
            return;
        }

        List<AlertEvent> alerts = orchestrator.process(event);
        for (AlertEvent alert : alerts) {
            collector.collect(alert);
        }
    }

    private AnomalyScoreEvent extractAnomalyScoreEvent(List<TimestreamPoint> points) {
        AnomalyScoreEvent event = new AnomalyScoreEvent();
        boolean hasScores = false;
        long timestamp = 0;

        Map<String, Double> measures = new HashMap<>();
        Map<String, String> dimensions = new HashMap<>(defaultDimensions);

        for (TimestreamPoint point : points) {
            if (timestamp == 0) {
                timestamp = point.getTime();
            }

            String measureName = point.getMeasureName();
            String measureValue = point.getMeasureValue();

            if (point.getDimensions() != null) {
                dimensions.putAll(point.getDimensions());
            }

            try {
                double value = Double.parseDouble(measureValue);
                measures.put(measureName, value);

                if ("anomaly_score_stream6".equals(measureName)) {
                    event.setStream6Score(value);
                    hasScores = true;
                } else if ("anomaly_score_stream9".equals(measureName)) {
                    event.setStream9Score(value);
                    hasScores = true;
                } else if ("anomaly_score_stream_11".equals(measureName) ||
                        "anomaly_score_stream11".equals(measureName)) {
                    event.setStream11Score(value);
                    hasScores = true;
                }
            } catch (NumberFormatException e) {
                logger.debug("Non-numeric measure value: {} = {}", measureName, measureValue);
            }
        }

        if (!hasScores) {
            return null;
        }

        event.setTimestamp(timestamp > 0 ? timestamp : Instant.now().toEpochMilli());
        event.setDimensions(dimensions);

        return event;
    }

    public static AnomalyScoreEvent extractFromJson(String jsonString, long timestamp) {
        AnomalyScoreEvent event = new AnomalyScoreEvent();
        event.setTimestamp(timestamp);

        try {
            HashMap<String, Object> map = new Gson().fromJson(jsonString,
                    new TypeToken<HashMap<String, JsonElement>>() {
                    }.getType());

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();

                try {
                    double doubleValue = Double.parseDouble(value);
                    if ("anomaly_score_stream6".equals(key)) {
                        event.setStream6Score(doubleValue);
                    } else if ("anomaly_score_stream9".equals(key)) {
                        event.setStream9Score(doubleValue);
                    } else if ("anomaly_score_stream_11".equals(key) || "anomaly_score_stream11".equals(key)) {
                        event.setStream11Score(doubleValue);
                    } else if (key.startsWith("xmeas_")) {
                        event.addDimension(key, value);
                    }
                } catch (NumberFormatException e) {
                    if (!value.isEmpty() && !"null".equalsIgnoreCase(value)) {
                        event.addDimension(key, value);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse anomaly score JSON: {}", e.getMessage(), e);
            return null;
        }

        return event;
    }

    public AlertOrchestrator getOrchestrator() {
        return orchestrator;
    }

    public void addRule(AlertRule rule) {
        if (orchestrator != null) {
            orchestrator.registerRule(rule);
        } else {
            initialRules.add(rule);
        }
    }

    @Override
    public void close() throws Exception {
        if (orchestrator != null) {
            orchestrator.shutdown();
        }
    }
}
