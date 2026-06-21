/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class AlertRule {
    public enum Operator {
        AND, OR
    }

    public enum Comparator {
        GT, GTE, LT, LTE, EQ, NE
    }

    public enum Severity {
        INFO, WARNING, CRITICAL, FATAL
    }

    public static class Condition {
        private String streamName;
        private Comparator comparator;
        private double threshold;

        public Condition() {}

        public Condition(String streamName, Comparator comparator, double threshold) {
            this.streamName = streamName;
            this.comparator = comparator;
            this.threshold = threshold;
        }

        public String getStreamName() {
            return streamName;
        }

        public void setStreamName(String streamName) {
            this.streamName = streamName;
        }

        public Comparator getComparator() {
            return comparator;
        }

        public void setComparator(Comparator comparator) {
            this.comparator = comparator;
        }

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }

        public boolean evaluate(double score) {
            double epsilon = 1e-9;
            switch (comparator) {
                case GT: return score > threshold + epsilon;
                case GTE: return score >= threshold - epsilon;
                case LT: return score < threshold - epsilon;
                case LTE: return score <= threshold + epsilon;
                case EQ: return Math.abs(score - threshold) < epsilon;
                case NE: return Math.abs(score - threshold) >= epsilon;
                default: return false;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Condition condition = (Condition) o;
            return Double.compare(condition.threshold, threshold) == 0 &&
                    Objects.equals(streamName, condition.streamName) &&
                    comparator == condition.comparator;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamName, comparator, threshold);
        }
    }

    public static class EscalationLevel {
        private int level;
        private int triggerCount;
        private long timeWindowMs;
        private Severity severity;
        private String notificationChannel;

        public EscalationLevel() {}

        public EscalationLevel(int level, int triggerCount, long timeWindowMs, Severity severity, String notificationChannel) {
            this.level = level;
            this.triggerCount = triggerCount;
            this.timeWindowMs = timeWindowMs;
            this.severity = severity;
            this.notificationChannel = notificationChannel;
        }

        public int getLevel() {
            return level;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public int getTriggerCount() {
            return triggerCount;
        }

        public void setTriggerCount(int triggerCount) {
            this.triggerCount = triggerCount;
        }

        public long getTimeWindowMs() {
            return timeWindowMs;
        }

        public void setTimeWindowMs(long timeWindowMs) {
            this.timeWindowMs = timeWindowMs;
        }

        public Severity getSeverity() {
            return severity;
        }

        public void setSeverity(Severity severity) {
            this.severity = severity;
        }

        public String getNotificationChannel() {
            return notificationChannel;
        }

        public void setNotificationChannel(String notificationChannel) {
            this.notificationChannel = notificationChannel;
        }
    }

    private String ruleId;
    private String ruleName;
    private String description;
    private Operator operator;
    private List<Condition> conditions;
    private Severity severity;
    private long jitterSuppressionWindowMs;
    private int jitterMinOccurrences;
    private long autoRecoveryWindowMs;
    private int autoRecoveryThreshold;
    private List<EscalationLevel> escalationLevels;
    private long version;
    private long createdAt;
    private long updatedAt;
    private boolean enabled;

    public AlertRule() {
        this.ruleId = UUID.randomUUID().toString();
        this.conditions = new ArrayList<>();
        this.escalationLevels = new ArrayList<>();
        this.createdAt = System.currentTimeMillis();
        this.updatedAt = this.createdAt;
        this.version = 1;
        this.enabled = true;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public void addCondition(Condition condition) {
        this.conditions.add(condition);
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public long getJitterSuppressionWindowMs() {
        return jitterSuppressionWindowMs;
    }

    public void setJitterSuppressionWindowMs(long jitterSuppressionWindowMs) {
        this.jitterSuppressionWindowMs = jitterSuppressionWindowMs;
    }

    public int getJitterMinOccurrences() {
        return jitterMinOccurrences;
    }

    public void setJitterMinOccurrences(int jitterMinOccurrences) {
        this.jitterMinOccurrences = jitterMinOccurrences;
    }

    public long getAutoRecoveryWindowMs() {
        return autoRecoveryWindowMs;
    }

    public void setAutoRecoveryWindowMs(long autoRecoveryWindowMs) {
        this.autoRecoveryWindowMs = autoRecoveryWindowMs;
    }

    public int getAutoRecoveryThreshold() {
        return autoRecoveryThreshold;
    }

    public void setAutoRecoveryThreshold(int autoRecoveryThreshold) {
        this.autoRecoveryThreshold = autoRecoveryThreshold;
    }

    public List<EscalationLevel> getEscalationLevels() {
        return escalationLevels;
    }

    public void setEscalationLevels(List<EscalationLevel> escalationLevels) {
        this.escalationLevels = escalationLevels;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean evaluate(AnomalyScoreEvent event) {
        if (!enabled || conditions.isEmpty()) {
            return false;
        }

        if (operator == Operator.AND) {
            for (Condition condition : conditions) {
                double score = event.getScoreByStream(condition.getStreamName());
                if (!condition.evaluate(score)) {
                    return false;
                }
            }
            return true;
        } else {
            for (Condition condition : conditions) {
                double score = event.getScoreByStream(condition.getStreamName());
                if (condition.evaluate(score)) {
                    return true;
                }
            }
            return false;
        }
    }

    public int calculateFingerprint() {
        return Objects.hash(ruleId, version, operator, conditions, severity,
                jitterSuppressionWindowMs, jitterMinOccurrences,
                autoRecoveryWindowMs, autoRecoveryThreshold, escalationLevels);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlertRule alertRule = (AlertRule) o;
        return calculateFingerprint() == alertRule.calculateFingerprint();
    }

    @Override
    public int hashCode() {
        return calculateFingerprint();
    }
}
