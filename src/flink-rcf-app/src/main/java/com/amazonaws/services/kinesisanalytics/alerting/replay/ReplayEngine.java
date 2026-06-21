/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.replay;

import com.amazonaws.services.kinesisanalytics.alerting.engine.AlertStateManager;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine.RuleEvaluationResult;
import com.amazonaws.services.kinesisanalytics.alerting.engine.SilenceManager;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.AlertEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.EventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.RuleVersionedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplayEngine {
    private static final Logger logger = LoggerFactory.getLogger(ReplayEngine.class);

    private final EventStore eventStore;
    private final AlertEventStore alertEventStore;
    private final RuleVersionedStore ruleVersionedStore;
    private final RuleEngine ruleEngine;
    private final AlertStateManager stateManager;
    private final SilenceManager silenceManager;

    public ReplayEngine(EventStore eventStore, AlertEventStore alertEventStore,
                        RuleVersionedStore ruleVersionedStore) {
        this.eventStore = eventStore;
        this.alertEventStore = alertEventStore;
        this.ruleVersionedStore = ruleVersionedStore;
        this.silenceManager = new SilenceManager();
        this.ruleEngine = new RuleEngine();
        this.stateManager = new AlertStateManager(silenceManager);
    }

    public ReplayResult replay(long startTime, long endTime) {
        return replay(startTime, endTime, null, false);
    }

    public ReplayResult replay(long startTime, long endTime, String ruleId, boolean useLatestRule) {
        logger.info("Starting replay: timeRange=[{}, {}], ruleId={}, useLatestRule={}",
                startTime, endTime, ruleId, useLatestRule);

        ReplayResult result = new ReplayResult();
        result.setStartTime(startTime);
        result.setEndTime(endTime);
        result.setRuleId(ruleId);
        result.setUseLatestRule(useLatestRule);

        long replayStart = System.currentTimeMillis();

        List<AnomalyScoreEvent> events = eventStore.getEventsByTimeRange(startTime, endTime);
        result.setTotalEventsProcessed(events.size());
        logger.info("Loaded {} events for replay", events.size());

        stateManager.clearAllStates();
        ruleEngine.getAllRules().forEach(r -> ruleEngine.removeRule(r.getRuleId()));

        Map<String, AlertRule> activeRules = new HashMap<>();
        List<AlertEvent> generatedAlerts = new ArrayList<>();
        Map<String, Integer> ruleTriggerCounts = new HashMap<>();

        int processedCount = 0;
        for (AnomalyScoreEvent event : events) {
            processedCount++;

            List<AlertRule> rulesForEvent = getRulesForEvent(event, ruleId, useLatestRule);

            for (AlertRule rule : rulesForEvent) {
                if (!activeRules.containsKey(rule.getRuleId()) ||
                        activeRules.get(rule.getRuleId()).getVersion() != rule.getVersion()) {
                    ruleEngine.registerRule(rule);
                    activeRules.put(rule.getRuleId(), rule);
                    logger.debug("Activated rule {} version {} for replay", rule.getRuleId(), rule.getVersion());
                }

                RuleEvaluationResult evalResult = ruleEngine.evaluate(rule, event);
                List<AlertEvent> alertEvents = stateManager.process(evalResult, rule);

                for (AlertEvent alert : alertEvents) {
                    alert.setFingerprint(AlertEvent.computeFingerprint(rule.getRuleId(), event.getDimensions()));
                    generatedAlerts.add(alert);
                    ruleTriggerCounts.merge(rule.getRuleId(), 1, Integer::sum);
                }
            }

            if (processedCount % 1000 == 0) {
                logger.debug("Replay progress: {}/{} events", processedCount, events.size());
            }
        }

        result.setGeneratedAlerts(generatedAlerts);
        result.setRuleTriggerCounts(ruleTriggerCounts);
        result.setTotalAlertsGenerated(generatedAlerts.size());

        if (!useLatestRule && ruleId == null) {
            List<AlertEvent> originalAlerts = alertEventStore.getAlertEventsByTimeRange(startTime, endTime);
            result.setOriginalAlerts(originalAlerts);
            result.setComparisonResult(compareAlerts(originalAlerts, generatedAlerts));
        }

        long replayDuration = System.currentTimeMillis() - replayStart;
        result.setReplayDurationMs(replayDuration);

        logger.info("Replay completed: {} events, {} alerts generated, duration={}ms",
                events.size(), generatedAlerts.size(), replayDuration);

        return result;
    }

    private List<AlertRule> getRulesForEvent(AnomalyScoreEvent event, String ruleId, boolean useLatestRule) {
        List<AlertRule> rules;

        if (useLatestRule) {
            if (ruleId != null) {
                rules = ruleVersionedStore.getRule(ruleId)
                        .map(List::of)
                        .orElse(new ArrayList<>());
            } else {
                rules = ruleVersionedStore.getAllRules();
            }
        } else {
            if (ruleId != null) {
                rules = ruleVersionedStore.getRuleActiveAtTime(ruleId, event.getTimestamp())
                        .map(List::of)
                        .orElse(new ArrayList<>());
            } else {
                rules = ruleVersionedStore.getRulesActiveAtTime(event.getTimestamp());
            }
        }

        return rules;
    }

    private ReplayComparison compareAlerts(List<AlertEvent> original, List<AlertEvent> replayed) {
        ReplayComparison comparison = new ReplayComparison();
        comparison.setOriginalCount(original.size());
        comparison.setReplayedCount(replayed.size());

        Map<String, AlertEvent> originalByKey = new HashMap<>();
        for (AlertEvent alert : original) {
            String key = alert.getFingerprint() + "|" + alert.getTimestamp() + "|" + alert.getAlertType();
            originalByKey.put(key, alert);
        }

        Map<String, AlertEvent> replayedByKey = new HashMap<>();
        for (AlertEvent alert : replayed) {
            String key = alert.getFingerprint() + "|" + alert.getTimestamp() + "|" + alert.getAlertType();
            replayedByKey.put(key, alert);
        }

        List<AlertEvent> matching = new ArrayList<>();
        List<AlertEvent> onlyInOriginal = new ArrayList<>();
        List<AlertEvent> onlyInReplayed = new ArrayList<>();
        List<AlertDifference> differences = new ArrayList<>();

        for (Map.Entry<String, AlertEvent> entry : originalByKey.entrySet()) {
            String key = entry.getKey();
            AlertEvent origAlert = entry.getValue();
            AlertEvent replayAlert = replayedByKey.get(key);

            if (replayAlert != null) {
                matching.add(origAlert);
                AlertDifference diff = compareAlertDetails(origAlert, replayAlert);
                if (diff != null) {
                    differences.add(diff);
                }
            } else {
                onlyInOriginal.add(origAlert);
            }
        }

        for (Map.Entry<String, AlertEvent> entry : replayedByKey.entrySet()) {
            if (!originalByKey.containsKey(entry.getKey())) {
                onlyInReplayed.add(entry.getValue());
            }
        }

        comparison.setMatchingCount(matching.size());
        comparison.setOnlyInOriginal(onlyInOriginal);
        comparison.setOnlyInReplayed(onlyInReplayed);
        comparison.setDifferences(differences);
        comparison.setConsistent(differences.isEmpty() && onlyInOriginal.isEmpty() && onlyInReplayed.isEmpty());

        logger.info("Replay comparison: original={}, replayed={}, matching={}, onlyOriginal={}, onlyReplayed={}, differences={}",
                original.size(), replayed.size(), matching.size(),
                onlyInOriginal.size(), onlyInReplayed.size(), differences.size());

        return comparison;
    }

    private AlertDifference compareAlertDetails(AlertEvent original, AlertEvent replayed) {
        AlertDifference diff = new AlertDifference();
        diff.setAlertId(original.getAlertId());
        diff.setFingerprint(original.getFingerprint());
        diff.setTimestamp(original.getTimestamp());

        Map<String, String> fields = new HashMap<>();

        if (original.getSeverity() != replayed.getSeverity()) {
            fields.put("severity", original.getSeverity() + " -> " + replayed.getSeverity());
        }
        if (original.getCurrentLevel() != replayed.getCurrentLevel()) {
            fields.put("level", original.getCurrentLevel() + " -> " + replayed.getCurrentLevel());
        }
        if (original.getTriggerCount() != replayed.getTriggerCount()) {
            fields.put("triggerCount", original.getTriggerCount() + " -> " + replayed.getTriggerCount());
        }
        if (original.getRuleVersion() != replayed.getRuleVersion()) {
            fields.put("ruleVersion", original.getRuleVersion() + " -> " + replayed.getRuleVersion());
        }

        if (fields.isEmpty()) {
            return null;
        }
        diff.setDifferentFields(fields);
        return diff;
    }

    public static class ReplayResult {
        private long startTime;
        private long endTime;
        private String ruleId;
        private boolean useLatestRule;
        private int totalEventsProcessed;
        private int totalAlertsGenerated;
        private long replayDurationMs;
        private List<AlertEvent> generatedAlerts;
        private List<AlertEvent> originalAlerts;
        private Map<String, Integer> ruleTriggerCounts;
        private ReplayComparison comparisonResult;

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public String getRuleId() {
            return ruleId;
        }

        public void setRuleId(String ruleId) {
            this.ruleId = ruleId;
        }

        public boolean isUseLatestRule() {
            return useLatestRule;
        }

        public void setUseLatestRule(boolean useLatestRule) {
            this.useLatestRule = useLatestRule;
        }

        public int getTotalEventsProcessed() {
            return totalEventsProcessed;
        }

        public void setTotalEventsProcessed(int totalEventsProcessed) {
            this.totalEventsProcessed = totalEventsProcessed;
        }

        public int getTotalAlertsGenerated() {
            return totalAlertsGenerated;
        }

        public void setTotalAlertsGenerated(int totalAlertsGenerated) {
            this.totalAlertsGenerated = totalAlertsGenerated;
        }

        public long getReplayDurationMs() {
            return replayDurationMs;
        }

        public void setReplayDurationMs(long replayDurationMs) {
            this.replayDurationMs = replayDurationMs;
        }

        public List<AlertEvent> getGeneratedAlerts() {
            return generatedAlerts;
        }

        public void setGeneratedAlerts(List<AlertEvent> generatedAlerts) {
            this.generatedAlerts = generatedAlerts;
        }

        public List<AlertEvent> getOriginalAlerts() {
            return originalAlerts;
        }

        public void setOriginalAlerts(List<AlertEvent> originalAlerts) {
            this.originalAlerts = originalAlerts;
        }

        public Map<String, Integer> getRuleTriggerCounts() {
            return ruleTriggerCounts;
        }

        public void setRuleTriggerCounts(Map<String, Integer> ruleTriggerCounts) {
            this.ruleTriggerCounts = ruleTriggerCounts;
        }

        public ReplayComparison getComparisonResult() {
            return comparisonResult;
        }

        public void setComparisonResult(ReplayComparison comparisonResult) {
            this.comparisonResult = comparisonResult;
        }
    }

    public static class ReplayComparison {
        private int originalCount;
        private int replayedCount;
        private int matchingCount;
        private List<AlertEvent> onlyInOriginal;
        private List<AlertEvent> onlyInReplayed;
        private List<AlertDifference> differences;
        private boolean consistent;

        public int getOriginalCount() {
            return originalCount;
        }

        public void setOriginalCount(int originalCount) {
            this.originalCount = originalCount;
        }

        public int getReplayedCount() {
            return replayedCount;
        }

        public void setReplayedCount(int replayedCount) {
            this.replayedCount = replayedCount;
        }

        public int getMatchingCount() {
            return matchingCount;
        }

        public void setMatchingCount(int matchingCount) {
            this.matchingCount = matchingCount;
        }

        public List<AlertEvent> getOnlyInOriginal() {
            return onlyInOriginal;
        }

        public void setOnlyInOriginal(List<AlertEvent> onlyInOriginal) {
            this.onlyInOriginal = onlyInOriginal;
        }

        public List<AlertEvent> getOnlyInReplayed() {
            return onlyInReplayed;
        }

        public void setOnlyInReplayed(List<AlertEvent> onlyInReplayed) {
            this.onlyInReplayed = onlyInReplayed;
        }

        public List<AlertDifference> getDifferences() {
            return differences;
        }

        public void setDifferences(List<AlertDifference> differences) {
            this.differences = differences;
        }

        public boolean isConsistent() {
            return consistent;
        }

        public void setConsistent(boolean consistent) {
            this.consistent = consistent;
        }
    }

    public static class AlertDifference {
        private String alertId;
        private String fingerprint;
        private long timestamp;
        private Map<String, String> differentFields;

        public String getAlertId() {
            return alertId;
        }

        public void setAlertId(String alertId) {
            this.alertId = alertId;
        }

        public String getFingerprint() {
            return fingerprint;
        }

        public void setFingerprint(String fingerprint) {
            this.fingerprint = fingerprint;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Map<String, String> getDifferentFields() {
            return differentFields;
        }

        public void setDifferentFields(Map<String, String> differentFields) {
            this.differentFields = differentFields;
        }
    }
}
