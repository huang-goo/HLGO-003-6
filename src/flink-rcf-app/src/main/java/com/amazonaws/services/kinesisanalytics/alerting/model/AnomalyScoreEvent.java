/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.model;

import java.util.HashMap;
import java.util.Map;

public class AnomalyScoreEvent {
    private long timestamp;
    private double stream6Score;
    private double stream9Score;
    private double stream11Score;
    private Map<String, String> dimensions;
    private String rawPayload;

    public AnomalyScoreEvent() {
        this.dimensions = new HashMap<>();
    }

    public AnomalyScoreEvent(long timestamp, double stream6Score, double stream9Score, double stream11Score) {
        this.timestamp = timestamp;
        this.stream6Score = stream6Score;
        this.stream9Score = stream9Score;
        this.stream11Score = stream11Score;
        this.dimensions = new HashMap<>();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getStream6Score() {
        return stream6Score;
    }

    public void setStream6Score(double stream6Score) {
        this.stream6Score = stream6Score;
    }

    public double getStream9Score() {
        return stream9Score;
    }

    public void setStream9Score(double stream9Score) {
        this.stream9Score = stream9Score;
    }

    public double getStream11Score() {
        return stream11Score;
    }

    public void setStream11Score(double stream11Score) {
        this.stream11Score = stream11Score;
    }

    public double getScoreByStream(String streamName) {
        switch (streamName.toLowerCase()) {
            case "stream6":
            case "anomaly_score_stream6":
                return stream6Score;
            case "stream9":
            case "anomaly_score_stream9":
                return stream9Score;
            case "stream11":
            case "anomaly_score_stream11":
            case "anomaly_score_stream_11":
                return stream11Score;
            default:
                throw new IllegalArgumentException("Unknown stream: " + streamName);
        }
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = new HashMap<>(dimensions);
    }

    public void addDimension(String key, String value) {
        this.dimensions.put(key, value);
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    @Override
    public String toString() {
        return "AnomalyScoreEvent{" +
                "timestamp=" + timestamp +
                ", stream6Score=" + stream6Score +
                ", stream9Score=" + stream9Score +
                ", stream11Score=" + stream11Score +
                ", dimensions=" + dimensions +
                '}';
    }
}
