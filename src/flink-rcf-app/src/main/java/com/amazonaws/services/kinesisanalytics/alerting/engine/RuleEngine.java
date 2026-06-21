/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.engine;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Condition;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RuleEngine {
    private static final Logger logger = LoggerFactory.getLogger(RuleEngine.class);

    private final Map<String, AlertRule> rules;
    private final Map<String, Long> ruleVersions;

    public RuleEngine() {
        this.rules = new ConcurrentHashMap<>();
        this.ruleVersions = new ConcurrentHashMap<>();
    }

    public void registerRule(AlertRule rule) {
        if (rule == null || rule.getRuleId() == null) {
            throw new IllegalArgumentException("Rule and ruleId must not be null");
        }
        String ruleId = rule.getRuleId();
        AlertRule existing = rules.get(ruleId);
        if (existing != null) {
            if (existing.equals(rule)) {
                logger.debug("Rule {} unchanged, skipping update", ruleId);
                return;
            }
            rule.setVersion(existing.getVersion() + 1);
            logger.info("Updating rule {} from version {} to {}",
                    ruleId, existing.getVersion(), rule.getVersion());
        } else {
            logger.info("Registering new rule {}", ruleId);
        }
        rule.setUpdatedAt(System.currentTimeMillis());
        rules.put(ruleId, rule);
        ruleVersions.put(ruleId, rule.getVersion());
    }

    public void removeRule(String ruleId) {
        rules.remove(ruleId);
        ruleVersions.remove(ruleId);
        logger.info("Removed rule {}", ruleId);
    }

    public AlertRule getRule(String ruleId) {
        return rules.get(ruleId);
    }

    public List<AlertRule> getAllRules() {
        return new ArrayList<>(rules.values());
    }

    public long getRuleVersion(String ruleId) {
        return ruleVersions.getOrDefault(ruleId, 0L);
    }

    public List<RuleEvaluationResult> evaluateAll(AnomalyScoreEvent event) {
        List<RuleEvaluationResult> results = new ArrayList<>();
        for (AlertRule rule : rules.values()) {
            if (!rule.isEnabled()) {
                continue;
            }
            RuleEvaluationResult result = evaluate(rule, event);
            results.add(result);
        }
        return results;
    }

    public RuleEvaluationResult evaluate(AlertRule rule, AnomalyScoreEvent event) {
        RuleEvaluationResult result = new RuleEvaluationResult();
        result.setRuleId(rule.getRuleId());
        result.setRuleVersion(rule.getVersion());
        result.setEvent(event);
        result.setTimestamp(event.getTimestamp());

        if (!rule.isEnabled()) {
            result.setTriggered(false);
            result.setEvaluationDetails("Rule is disabled");
            return result;
        }

        try {
            boolean matched = rule.evaluate(event);
            result.setTriggered(matched);

            StringBuilder details = new StringBuilder();
            details.append("Operator: ").append(rule.getOperator()).append("; ");
            List<ConditionResult> conditionResults = new ArrayList<>();
            for (Condition condition : rule.getConditions()) {
                ConditionResult cr = evaluateCondition(condition, event);
                conditionResults.add(cr);
                details.append(cr.getStreamName())
                        .append(": ")
                        .append(cr.getActualValue())
                        .append(" ")
                        .append(cr.getComparator())
                        .append(" ")
                        .append(cr.getThreshold())
                        .append(" = ")
                        .append(cr.isMatched())
                        .append("; ");
            }
            result.setConditionResults(conditionResults);
            result.setEvaluationDetails(details.toString());

            logger.debug("Rule {} evaluated: {} = {}", rule.getRuleId(), details, matched);

        } catch (Exception e) {
            logger.error("Error evaluating rule {}: {}", rule.getRuleId(), e.getMessage(), e);
            result.setTriggered(false);
            result.setError(e.getMessage());
            result.setEvaluationDetails("Error: " + e.getMessage());
        }

        return result;
    }

    private ConditionResult evaluateCondition(Condition condition, AnomalyScoreEvent event) {
        ConditionResult result = new ConditionResult();
        result.setStreamName(condition.getStreamName());
        result.setComparator(condition.getComparator());
        result.setThreshold(condition.getThreshold());

        double score = event.getScoreByStream(condition.getStreamName());
        result.setActualValue(score);
        result.setMatched(condition.evaluate(score));

        return result;
    }

    public static class ConditionResult {
        private String streamName;
        private AlertRule.Comparator comparator;
        private double threshold;
        private double actualValue;
        private boolean matched;

        public String getStreamName() {
            return streamName;
        }

        public void setStreamName(String streamName) {
            this.streamName = streamName;
        }

        public AlertRule.Comparator getComparator() {
            return comparator;
        }

        public void setComparator(AlertRule.Comparator comparator) {
            this.comparator = comparator;
        }

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }

        public double getActualValue() {
            return actualValue;
        }

        public void setActualValue(double actualValue) {
            this.actualValue = actualValue;
        }

        public boolean isMatched() {
            return matched;
        }

        public void setMatched(boolean matched) {
            this.matched = matched;
        }

        @Override
        public String toString() {
            return streamName + ": " + actualValue + " " + comparator + " " + threshold + " = " + matched;
        }
    }

    public static class RuleEvaluationResult {
        private String ruleId;
        private long ruleVersion;
        private AnomalyScoreEvent event;
        private long timestamp;
        private boolean triggered;
        private String evaluationDetails;
        private String error;
        private List<ConditionResult> conditionResults;

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

        public AnomalyScoreEvent getEvent() {
            return event;
        }

        public void setEvent(AnomalyScoreEvent event) {
            this.event = event;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public boolean isTriggered() {
            return triggered;
        }

        public void setTriggered(boolean triggered) {
            this.triggered = triggered;
        }

        public String getEvaluationDetails() {
            return evaluationDetails;
        }

        public void setEvaluationDetails(String evaluationDetails) {
            this.evaluationDetails = evaluationDetails;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public List<ConditionResult> getConditionResults() {
            return conditionResults;
        }

        public void setConditionResults(List<ConditionResult> conditionResults) {
            this.conditionResults = conditionResults;
        }

        public boolean hasError() {
            return error != null && !error.isEmpty();
        }

        @Override
        public String toString() {
            return "RuleEvaluationResult{" +
                    "ruleId='" + ruleId + '\'' +
                    ", triggered=" + triggered +
                    ", details='" + evaluationDetails + '\'' +
                    '}';
        }
    }
}
