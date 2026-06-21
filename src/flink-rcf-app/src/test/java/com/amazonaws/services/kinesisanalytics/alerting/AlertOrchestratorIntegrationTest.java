/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Comparator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Condition;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Operator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Severity;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryAlertEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryRuleVersionedStore;
import com.amazonaws.services.kinesisanalytics.alerting.replay.ReplayEngine;
import com.amazonaws.services.kinesisanalytics.alerting.replay.ReplayEngine.ReplayComparison;
import com.amazonaws.services.kinesisanalytics.alerting.replay.ReplayEngine.ReplayResult;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AlertOrchestratorIntegrationTest {

    private AlertOrchestrator orchestrator;
    private InMemoryEventStore eventStore;
    private InMemoryAlertEventStore alertEventStore;
    private InMemoryRuleVersionedStore ruleVersionedStore;
    private ReplayEngine replayEngine;
    private long baseTime;

    @Before
    public void setUp() {
        eventStore = new InMemoryEventStore();
        alertEventStore = new InMemoryAlertEventStore();
        ruleVersionedStore = new InMemoryRuleVersionedStore();
        orchestrator = new AlertOrchestrator(eventStore, alertEventStore, ruleVersionedStore);
        replayEngine = new ReplayEngine(eventStore, alertEventStore, ruleVersionedStore);
        baseTime = System.currentTimeMillis();
    }

    @Test
    public void testEndToEndAlertProcessing() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("integration-test-rule");
        rule.setRuleName("Integration Test Rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(2);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(2);
        rule.setUpdatedAt(baseTime - TimeUnit.MINUTES.toMillis(1));
        orchestrator.registerRule(rule);

        long timestamp = baseTime;
        int alertCount = 0;

        for (int i = 0; i < 5; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
            event.addDimension("factory", "test-factory");
            List<AlertEvent> alerts = orchestrator.process(event);

            if (i == 0) {
                assertEquals("First event should not trigger - jitter suppression", 0, alerts.size());
            } else if (i == 1) {
                assertEquals("Second event should trigger", 1, alerts.size());
                assertEquals(AlertEvent.AlertType.TRIGGER, alerts.get(0).getAlertType());
                assertEquals(Severity.WARNING, alerts.get(0).getSeverity());
                alertCount++;
            } else if (i < 4) {
                assertTrue("Subsequent events should produce updates", alerts.size() > 0);
                alertCount++;
            }

            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        for (int i = 0; i < 2; i++) {
            AnomalyScoreEvent okEvent = new AnomalyScoreEvent(timestamp, 0.5, 0.5, 0.5);
            okEvent.addDimension("factory", "test-factory");
            List<AlertEvent> alerts = orchestrator.process(okEvent);

            if (i == 1) {
                assertTrue("Should recover after 2 OK events",
                        alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.RECOVER));
            }

            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        assertEquals("Should have persisted events", 7, eventStore.getEventCount());
        assertTrue("Should have persisted alerts", alertEventStore.getAlertEventCount() > 0);
    }

    @Test
    public void testReplayConsistency() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("replay-test-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(3);
        rule.setUpdatedAt(baseTime - TimeUnit.MINUTES.toMillis(1));
        orchestrator.registerRule(rule);

        long startTime = baseTime;
        long timestamp = startTime;

        for (int i = 0; i < 10; i++) {
            double score = i < 5 ? 0.9 : 0.3;
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, score, 0.5, 0.5);
            event.addDimension("device", "device-001");
            orchestrator.process(event);
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        long endTime = timestamp;
        long originalAlertCount = alertEventStore.getAlertEventCount();

        ReplayResult replayResult = replayEngine.replay(startTime, endTime);
        assertEquals("Should process same number of events",
                10, replayResult.getTotalEventsProcessed());

        ReplayComparison comparison = replayResult.getComparisonResult();
        assertNotNull("Should have comparison result", comparison);
        assertEquals("Original alert count should match",
                originalAlertCount, comparison.getOriginalCount());
        assertEquals("Replayed alert count should match",
                originalAlertCount, comparison.getReplayedCount());
        assertTrue("Replay should be consistent", comparison.isConsistent());
        assertEquals("All alerts should match",
                originalAlertCount, comparison.getMatchingCount());
    }

    @Test
    public void testRuleChangeAndReplayWithLatestRule() {
        AlertRule originalRule = new AlertRule();
        originalRule.setRuleId("rule-change-test");
        originalRule.setOperator(Operator.AND);
        originalRule.addCondition(new Condition("stream6", Comparator.GT, 0.9));
        originalRule.setSeverity(Severity.WARNING);
        originalRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        originalRule.setJitterMinOccurrences(1);
        originalRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        originalRule.setAutoRecoveryThreshold(3);
        originalRule.setUpdatedAt(baseTime - TimeUnit.MINUTES.toMillis(1));
        orchestrator.registerRule(originalRule);

        long startTime = baseTime;
        long timestamp = startTime;

        for (int i = 0; i < 5; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.85, 0.5, 0.5);
            event.addDimension("device", "device-002");
            orchestrator.process(event);
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        long originalAlerts = alertEventStore.getAlertEventCount();
        assertEquals("Original threshold 0.9 should not trigger with 0.85 score", 0, originalAlerts);

        AlertRule updatedRule = new AlertRule();
        updatedRule.setRuleId("rule-change-test");
        updatedRule.setOperator(Operator.AND);
        updatedRule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        updatedRule.setSeverity(Severity.WARNING);
        updatedRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        updatedRule.setJitterMinOccurrences(1);
        updatedRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        updatedRule.setAutoRecoveryThreshold(3);
        updatedRule.setUpdatedAt(timestamp);
        orchestrator.registerRule(updatedRule);

        assertEquals("Rule version should increment", 2,
                ruleVersionedStore.getRule("rule-change-test").get().getVersion());

        ReplayResult replayWithLatest = replayEngine.replay(
                startTime, timestamp, "rule-change-test", true);

        assertTrue("With lower threshold, should generate alerts",
                replayWithLatest.getTotalAlertsGenerated() > 0);
        assertEquals("Should generate 5 alerts",
                5, replayWithLatest.getTotalAlertsGenerated());
    }

    @Test
    public void testReplayWithHistoricalRules() {
        long t0 = baseTime;

        AlertRule v1Rule = new AlertRule();
        v1Rule.setRuleId("historical-rule-test");
        v1Rule.setOperator(Operator.AND);
        v1Rule.addCondition(new Condition("stream6", Comparator.GT, 0.9));
        v1Rule.setSeverity(Severity.WARNING);
        v1Rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        v1Rule.setJitterMinOccurrences(1);
        v1Rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        v1Rule.setAutoRecoveryThreshold(3);
        v1Rule.setUpdatedAt(t0);
        orchestrator.registerRule(v1Rule);

        long t1 = t0 + TimeUnit.MINUTES.toMillis(5);
        for (int i = 0; i < 5; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(t1 + i * 60000, 0.85, 0.5, 0.5);
            orchestrator.process(event);
        }

        long t2 = t1 + TimeUnit.MINUTES.toMillis(5);
        AlertRule v2Rule = new AlertRule();
        v2Rule.setRuleId("historical-rule-test");
        v2Rule.setOperator(Operator.AND);
        v2Rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        v2Rule.setSeverity(Severity.WARNING);
        v2Rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        v2Rule.setJitterMinOccurrences(1);
        v2Rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        v2Rule.setAutoRecoveryThreshold(3);
        v2Rule.setUpdatedAt(t2);
        orchestrator.registerRule(v2Rule);

        long t3 = t2 + TimeUnit.MINUTES.toMillis(5);
        for (int i = 0; i < 5; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(t3 + i * 60000, 0.85, 0.5, 0.5);
            orchestrator.process(event);
        }

        long originalAlerts = alertEventStore.getAlertEventCount();
        assertEquals("Should have 5 alerts from v2 rule period", 5, originalAlerts);

        ReplayResult historicalReplay = replayEngine.replay(
                t0, t3 + TimeUnit.MINUTES.toMillis(5), "historical-rule-test", false);

        assertEquals("Should generate only 5 alerts with historical rules",
                5, historicalReplay.getTotalAlertsGenerated());

        ReplayResult latestReplay = replayEngine.replay(
                t0, t3 + TimeUnit.MINUTES.toMillis(5), "historical-rule-test", true);

        assertEquals("Should generate 10 alerts with latest rule applied to all history",
                10, latestReplay.getTotalAlertsGenerated());
    }

    @Test
    public void testMultipleDimensions() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("multi-dim-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream9", Comparator.GT, 0.8));
        rule.setSeverity(Severity.CRITICAL);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(3);
        rule.setUpdatedAt(baseTime - TimeUnit.MINUTES.toMillis(1));
        orchestrator.registerRule(rule);

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 2; j++) {
                AnomalyScoreEvent event = new AnomalyScoreEvent(
                        baseTime + i * 60000, 0.5, 0.9, 0.5);
                event.addDimension("device", "device-" + i);
                event.addDimension("region", "region-" + j);
                List<AlertEvent> alerts = orchestrator.process(event);

                if (j == 0) {
                    assertEquals("Should trigger for each unique dimension combo", 1, alerts.size());
                }
            }
        }

        assertEquals("Should have 6 firing states (3 devices x 2 regions)",
                6, orchestrator.getStateManager().getFiringStates().size());
    }

    @Test
    public void testPersistenceAndRecovery() {
        AlertOrchestrator orchestrator1 = new AlertOrchestrator(
                eventStore, alertEventStore, ruleVersionedStore);

        AlertRule rule = new AlertRule();
        rule.setRuleId("persistence-test-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream11", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(3);
        rule.setUpdatedAt(baseTime - TimeUnit.MINUTES.toMillis(1));
        orchestrator1.registerRule(rule);

        for (int i = 0; i < 5; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(
                    baseTime + i * 60000, 0.5, 0.5, 0.9);
            orchestrator1.process(event);
        }

        assertEquals(5, eventStore.getEventCount());
        assertEquals(5, alertEventStore.getAlertEventCount());
        assertEquals(1, ruleVersionedStore.getRuleCount());

        AlertOrchestrator orchestrator2 = new AlertOrchestrator(
                eventStore, alertEventStore, ruleVersionedStore);

        assertEquals("Rule should be restored from persistence",
                1, orchestrator2.getAllRules().size());
        assertEquals("persistence-test-rule",
                orchestrator2.getAllRules().get(0).getRuleId());
    }
}
