/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AlertEvent {
    public enum AlertType {
        TRIGGER, UPDATE, ESCALATE, RECOVER, SILENCED
    }

    private String alertId;
    private String ruleId;
    private long ruleVersion;
    private AlertType alertType;
    private AlertRule.Severity severity;
    private int currentLevel;
    private long timestamp;
    private long firstTriggerTimestamp;
    private long lastTriggerTimestamp;
    private int triggerCount;
    private AnomalyScoreEvent sourceEvent;
    private Map<String, String> dimensions;
    private String message;
    private boolean silenced;
    private String silencedBy;
    private long silencedUntil;
    private String notificationChannel;
    private String fingerprint;

    public AlertEvent() {
        this.alertId = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
        this.dimensions = new HashMap<>();
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public long getRuleVersion() {
        return ruleVersion;
    }

    public void setRuleVersion(long ruleVersion) {
        this.ruleVersion = ruleVersion;
    }

    public AlertType getAlertType() {
        return alertType;
    }

    public void setAlertType(AlertType alertType) {
        this.alertType = alertType;
    }

    public AlertRule.Severity getSeverity() {
        return severity;
    }

    public void setSeverity(AlertRule.Severity severity) {
        this.severity = severity;
    }

    public int getCurrentLevel() {
        return currentLevel;
    }

    public void setCurrentLevel(int currentLevel) {
        this.currentLevel = currentLevel;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getFirstTriggerTimestamp() {
        return firstTriggerTimestamp;
    }

    public void setFirstTriggerTimestamp(long firstTriggerTimestamp) {
        this.firstTriggerTimestamp = firstTriggerTimestamp;
    }

    public long getLastTriggerTimestamp() {
        return lastTriggerTimestamp;
    }

    public void setLastTriggerTimestamp(long lastTriggerTimestamp) {
        this.lastTriggerTimestamp = lastTriggerTimestamp;
    }

    public int getTriggerCount() {
        return triggerCount;
    }

    public void setTriggerCount(int triggerCount) {
        this.triggerCount = triggerCount;
    }

    public AnomalyScoreEvent getSourceEvent() {
        return sourceEvent;
    }

    public void setSourceEvent(AnomalyScoreEvent sourceEvent) {
        this.sourceEvent = sourceEvent;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = new HashMap<>(dimensions);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isSilenced() {
        return silenced;
    }

    public void setSilenced(boolean silenced) {
        this.silenced = silenced;
    }

    public String getSilencedBy() {
        return silencedBy;
    }

    public void setSilencedBy(String silencedBy) {
        this.silencedBy = silencedBy;
    }

    public long getSilencedUntil() {
        return silencedUntil;
    }

    public void setSilencedUntil(long silencedUntil) {
        this.silencedUntil = silencedUntil;
    }

    public String getNotificationChannel() {
        return notificationChannel;
    }

    public void setNotificationChannel(String notificationChannel) {
        this.notificationChannel = notificationChannel;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public static String computeFingerprint(String ruleId, Map<String, String> dimensions) {
        StringBuilder sb = new StringBuilder(ruleId);
        dimensions.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append("|").append(e.getKey()).append("=").append(e.getValue()));
        return UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString();
    }

    @Override
    public String toString() {
        return "AlertEvent{" +
                "alertId='" + alertId + '\'' +
                ", ruleId='" + ruleId + '\'' +
                ", alertType=" + alertType +
                ", severity=" + severity +
                ", timestamp=" + timestamp +
                ", triggerCount=" + triggerCount +
                ", message='" + message + '\'' +
                '}';
    }
}
