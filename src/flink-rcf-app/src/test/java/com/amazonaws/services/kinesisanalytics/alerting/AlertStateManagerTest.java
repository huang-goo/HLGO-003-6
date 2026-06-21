/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting;

import com.amazonaws.services.kinesisanalytics.alerting.engine.AlertStateManager;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine.RuleEvaluationResult;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Comparator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Condition;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.EscalationLevel;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Operator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Severity;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertState;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.SilenceRule;
import com.amazonaws.services.kinesisanalytics.alerting.engine.SilenceManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AlertStateManagerTest {

    private AlertStateManager stateManager;
    private RuleEngine ruleEngine;
    private SilenceManager silenceManager;
    private AlertRule testRule;
    private long baseTime;

    @Before
    public void setUp() {
        silenceManager = new SilenceManager();
        stateManager = new AlertStateManager(silenceManager);
        ruleEngine = new RuleEngine();

        stateManager.clearAllStates();
        silenceManager.clearAllRules();

        baseTime = System.currentTimeMillis();

        testRule = new AlertRule();
        testRule.setRuleId("test-jitter-rule");
        testRule.setRuleName("Test Jitter Rule");
        testRule.setOperator(Operator.AND);
        testRule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        testRule.setSeverity(Severity.WARNING);
        testRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        testRule.setJitterMinOccurrences(3);
        testRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        testRule.setAutoRecoveryThreshold(3);

        ruleEngine.registerRule(testRule);
    }

    @Test
    public void testJitterSuppression() {
        long timestamp = baseTime;

        for (int i = 0; i < 2; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
            RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, event);
            List<AlertEvent> alerts = stateManager.process(evalResult, testRule);

            if (i < 2) {
                assertTrue("Should not trigger alert before min occurrences", alerts.isEmpty());
            }
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        AnomalyScoreEvent thirdEvent = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
        RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, thirdEvent);
        List<AlertEvent> alerts = stateManager.process(evalResult, testRule);

        assertFalse("Should trigger alert after 3 occurrences", alerts.isEmpty());
        assertEquals(AlertEvent.AlertType.TRIGGER, alerts.get(0).getAlertType());
        assertEquals(Severity.WARNING, alerts.get(0).getSeverity());

        String fingerprint = alerts.get(0).getFingerprint();
        AlertState state = stateManager.getState(fingerprint);
        assertNotNull(state);
        assertEquals(AlertState.State.FIRING, state.getState());
        assertEquals(3, state.getTriggerCount());
    }

    @Test
    public void testJitterSuppressionOutsideWindow() {
        long timestamp = baseTime;

        AnomalyScoreEvent event1 = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
        RuleEvaluationResult eval1 = ruleEngine.evaluate(testRule, event1);
        stateManager.process(eval1, testRule);

        timestamp += TimeUnit.MINUTES.toMillis(6);

        AnomalyScoreEvent event2 = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
        RuleEvaluationResult eval2 = ruleEngine.evaluate(testRule, event2);
        List<AlertEvent> alerts2 = stateManager.process(eval2, testRule);
        assertTrue("Should not trigger - outside jitter window", alerts2.isEmpty());

        AnomalyScoreEvent event3 = new AnomalyScoreEvent(timestamp + 60000, 0.9, 0.5, 0.5);
        RuleEvaluationResult eval3 = ruleEngine.evaluate(testRule, event3);
        List<AlertEvent> alerts3 = stateManager.process(eval3, testRule);
        assertTrue("Still should not trigger - only 2 in window", alerts3.isEmpty());
    }

    @Test
    public void testAutoRecovery() {
        long timestamp = baseTime;

        for (int i = 0; i < 3; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
            RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, event);
            stateManager.process(evalResult, testRule);
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        String fingerprint = AlertEvent.computeFingerprint(testRule.getRuleId(), testRule.getConditions().get(0) != null ?
                new AnomalyScoreEvent().getDimensions() : new AnomalyScoreEvent().getDimensions());

        AlertState state = null;
        for (AlertState s : stateManager.getAllStates()) {
            if (s.getRuleId().equals(testRule.getRuleId())) {
                state = s;
                break;
            }
        }
        assertNotNull("State should exist", state);
        assertEquals(AlertState.State.FIRING, state.getState());
        fingerprint = state.getFingerprint();

        for (int i = 0; i < 3; i++) {
            AnomalyScoreEvent okEvent = new AnomalyScoreEvent(timestamp, 0.5, 0.5, 0.5);
            RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, okEvent);
            List<AlertEvent> alerts = stateManager.process(evalResult, testRule);

            timestamp += TimeUnit.MINUTES.toMillis(1);

            if (i < 2) {
                assertTrue("Should not recover before threshold",
                        alerts.stream().noneMatch(a -> a.getAlertType() == AlertEvent.AlertType.RECOVER));
            } else {
                assertTrue("Should emit recovery alert",
                        alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.RECOVER));
            }
        }

        state = stateManager.getState(fingerprint);
        assertEquals(AlertState.State.OK, state.getState());
    }

    @Test
    public void testEscalation() {
        AlertRule escalationRule = new AlertRule();
        escalationRule.setRuleId("escalation-rule");
        escalationRule.setOperator(Operator.AND);
        escalationRule.addCondition(new Condition("stream9", Comparator.GT, 0.8));
        escalationRule.setSeverity(Severity.WARNING);
        escalationRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        escalationRule.setJitterMinOccurrences(1);
        escalationRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        escalationRule.setAutoRecoveryThreshold(5);
        escalationRule.setEscalationLevels(Arrays.asList(
                new EscalationLevel(1, 3, TimeUnit.MINUTES.toMillis(10), Severity.CRITICAL, "email"),
                new EscalationLevel(2, 5, TimeUnit.MINUTES.toMillis(10), Severity.FATAL, "pager")
        ));
        ruleEngine.registerRule(escalationRule);

        long timestamp = baseTime;
        AlertState state = null;
        String fingerprint = null;

        for (int i = 0; i < 6; i++) {
            AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.5, 0.9, 0.5);
            RuleEvaluationResult evalResult = ruleEngine.evaluate(escalationRule, event);
            List<AlertEvent> alerts = stateManager.process(evalResult, escalationRule);

            if (i == 0) {
                assertFalse("First trigger should produce alert", alerts.isEmpty());
                assertEquals(AlertEvent.AlertType.TRIGGER, alerts.get(0).getAlertType());
                assertEquals(Severity.WARNING, alerts.get(0).getSeverity());
                assertEquals(0, alerts.get(0).getCurrentLevel());
                fingerprint = alerts.get(0).getFingerprint();
            } else if (i == 2) {
                assertTrue("Should escalate to level 1",
                        alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.ESCALATE));
                AlertEvent escalateEvent = alerts.stream()
                        .filter(a -> a.getAlertType() == AlertEvent.AlertType.ESCALATE)
                        .findFirst().get();
                assertEquals(1, escalateEvent.getCurrentLevel());
                assertEquals(Severity.CRITICAL, escalateEvent.getSeverity());
            } else if (i == 4) {
                assertTrue("Should escalate to level 2",
                        alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.ESCALATE));
                AlertEvent escalateEvent = alerts.stream()
                        .filter(a -> a.getAlertType() == AlertEvent.AlertType.ESCALATE)
                        .findFirst().get();
                assertEquals(2, escalateEvent.getCurrentLevel());
                assertEquals(Severity.FATAL, escalateEvent.getSeverity());
                assertEquals("pager", escalateEvent.getNotificationChannel());
            }

            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        state = stateManager.getState(fingerprint);
        assertEquals(2, state.getCurrentLevel());
        assertEquals(Severity.FATAL, state.getSeverity());
    }

    @Test
    public void testSilenceRule() {
        SilenceRule silence = new SilenceRule();
        silence.setRuleIdPattern("test-jitter-rule");
        silence.setStartTime(baseTime - TimeUnit.HOURS.toMillis(1));
        silence.setEndTime(baseTime + TimeUnit.HOURS.toMillis(1));
        silence.addDimensionMatcher("factory", "test-factory-1");
        silenceManager.addSilenceRule(silence);

        long timestamp = baseTime;
        AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
        event.addDimension("factory", "test-factory-1");

        RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, event);
        for (int i = 0; i < 5; i++) {
            List<AlertEvent> alerts = stateManager.process(evalResult, testRule);
            assertFalse("Should be silenced",
                    alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.TRIGGER));
            assertTrue("Should produce SILENCED alert",
                    alerts.stream().anyMatch(a -> a.getAlertType() == AlertEvent.AlertType.SILENCED));
        }

        String fingerprint = AlertEvent.computeFingerprint(testRule.getRuleId(), event.getDimensions());
        AlertState state = stateManager.getState(fingerprint);
        assertEquals(AlertState.State.SILENCED, state.getState());
        assertTrue(state.isSilenced());
    }

    @Test
    public void testStateTransitions() {
        long timestamp = baseTime;

        AnomalyScoreEvent event = new AnomalyScoreEvent(timestamp, 0.9, 0.5, 0.5);
        RuleEvaluationResult evalResult = ruleEngine.evaluate(testRule, event);

        for (int i = 0; i < 3; i++) {
            stateManager.process(evalResult, testRule);
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        AlertState state = null;
        for (AlertState s : stateManager.getAllStates()) {
            if (s.getRuleId().equals(testRule.getRuleId())) {
                state = s;
                break;
            }
        }
        assertNotNull(state);
        assertEquals(AlertState.State.FIRING, state.getState());

        for (int i = 0; i < 3; i++) {
            AnomalyScoreEvent okEvent = new AnomalyScoreEvent(timestamp, 0.5, 0.5, 0.5);
            RuleEvaluationResult okEval = ruleEngine.evaluate(testRule, okEvent);
            stateManager.process(okEval, testRule);
            timestamp += TimeUnit.MINUTES.toMillis(1);
        }

        assertEquals(AlertState.State.OK, state.getState());
    }

    @Test
    public void testMultipleRuleStates() {
        ruleEngine.removeRule("test-jitter-rule");

        AlertRule rule1 = new AlertRule();
        rule1.setRuleId("multi-rule-1");
        rule1.setOperator(Operator.AND);
        rule1.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule1.setSeverity(Severity.WARNING);
        rule1.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        rule1.setJitterMinOccurrences(1);
        rule1.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule1.setAutoRecoveryThreshold(3);
        ruleEngine.registerRule(rule1);

        AlertRule rule2 = new AlertRule();
        rule2.setRuleId("multi-rule-2");
        rule2.setOperator(Operator.AND);
        rule2.addCondition(new Condition("stream9", Comparator.GT, 0.8));
        rule2.setSeverity(Severity.CRITICAL);
        rule2.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(1));
        rule2.setJitterMinOccurrences(1);
        rule2.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule2.setAutoRecoveryThreshold(3);
        ruleEngine.registerRule(rule2);

        AnomalyScoreEvent event = new AnomalyScoreEvent(baseTime, 0.9, 0.9, 0.5);

        List<RuleEvaluationResult> results = ruleEngine.evaluateAll(event);
        for (RuleEvaluationResult evalResult : results) {
            AlertRule rule = ruleEngine.getRule(evalResult.getRuleId());
            if (rule != null) {
                stateManager.process(evalResult, rule);
            }
        }

        List<AlertState> firingStates = stateManager.getFiringStates();
        assertEquals(2, firingStates.size());
    }
}
