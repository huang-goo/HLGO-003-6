/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting;

import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine.RuleEvaluationResult;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Comparator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Condition;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Operator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Severity;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RuleEngineTest {

    private RuleEngine ruleEngine;
    private AnomalyScoreEvent highScoreEvent;
    private AnomalyScoreEvent lowScoreEvent;

    @Before
    public void setUp() {
        ruleEngine = new RuleEngine();

        highScoreEvent = new AnomalyScoreEvent(System.currentTimeMillis(), 0.9, 0.95, 0.95);
        highScoreEvent.addDimension("factory", "test-factory-1");

        lowScoreEvent = new AnomalyScoreEvent(System.currentTimeMillis(), 0.3, 0.2, 0.4);
        lowScoreEvent.addDimension("factory", "test-factory-1");
    }

    @Test
    public void testSingleRuleEvaluation() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("test-rule-1");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        RuleEvaluationResult result = ruleEngine.evaluate(rule, highScoreEvent);
        assertTrue("Rule should trigger for high score", result.isTriggered());
        assertEquals("test-rule-1", result.getRuleId());
        assertNotNull(result.getConditionResults());
        assertEquals(1, result.getConditionResults().size());
        assertTrue(result.getConditionResults().get(0).isMatched());

        result = ruleEngine.evaluate(rule, lowScoreEvent);
        assertFalse("Rule should not trigger for low score", result.isTriggered());
    }

    @Test
    public void testAndOperator() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("and-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.addCondition(new Condition("stream9", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        AnomalyScoreEvent bothHigh = new AnomalyScoreEvent(System.currentTimeMillis(), 0.9, 0.9, 0.5);
        RuleEvaluationResult result = ruleEngine.evaluate(rule, bothHigh);
        assertTrue("AND rule should trigger when both conditions met", result.isTriggered());

        AnomalyScoreEvent oneHigh = new AnomalyScoreEvent(System.currentTimeMillis(), 0.9, 0.5, 0.5);
        result = ruleEngine.evaluate(rule, oneHigh);
        assertFalse("AND rule should not trigger when only one condition met", result.isTriggered());
    }

    @Test
    public void testOrOperator() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("or-rule");
        rule.setOperator(Operator.OR);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.addCondition(new Condition("stream9", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        AnomalyScoreEvent oneHigh = new AnomalyScoreEvent(System.currentTimeMillis(), 0.9, 0.5, 0.5);
        RuleEvaluationResult result = ruleEngine.evaluate(rule, oneHigh);
        assertTrue("OR rule should trigger when at least one condition met", result.isTriggered());

        AnomalyScoreEvent bothLow = new AnomalyScoreEvent(System.currentTimeMillis(), 0.3, 0.2, 0.5);
        result = ruleEngine.evaluate(rule, bothLow);
        assertFalse("OR rule should not trigger when no conditions met", result.isTriggered());
    }

    @Test
    public void testEvaluateAll() {
        AlertRule rule1 = new AlertRule();
        rule1.setRuleId("rule-1");
        rule1.setOperator(Operator.AND);
        rule1.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule1.setSeverity(Severity.WARNING);
        rule1.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule1.setJitterMinOccurrences(1);
        rule1.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule1.setAutoRecoveryThreshold(1);

        AlertRule rule2 = new AlertRule();
        rule2.setRuleId("rule-2");
        rule2.setOperator(Operator.AND);
        rule2.addCondition(new Condition("stream9", Comparator.GT, 0.9));
        rule2.setSeverity(Severity.CRITICAL);
        rule2.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule2.setJitterMinOccurrences(1);
        rule2.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule2.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule1);
        ruleEngine.registerRule(rule2);

        List<RuleEvaluationResult> results = ruleEngine.evaluateAll(highScoreEvent);
        assertEquals(2, results.size());

        long triggeredCount = results.stream().filter(RuleEvaluationResult::isTriggered).count();
        assertEquals(2, triggeredCount);
    }

    @Test
    public void testRuleVersioning() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("versioned-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);
        assertEquals(1, ruleEngine.getRuleVersion("versioned-rule"));

        AlertRule updatedRule = new AlertRule();
        updatedRule.setRuleId("versioned-rule");
        updatedRule.setOperator(Operator.AND);
        updatedRule.addCondition(new Condition("stream6", Comparator.GT, 0.9));
        updatedRule.setSeverity(Severity.WARNING);
        updatedRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        updatedRule.setJitterMinOccurrences(1);
        updatedRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        updatedRule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(updatedRule);
        assertEquals(2, ruleEngine.getRuleVersion("versioned-rule"));

        AlertRule currentRule = ruleEngine.getRule("versioned-rule");
        assertEquals(2, currentRule.getVersion());
        assertEquals(0.9, currentRule.getConditions().get(0).getThreshold(), 0.001);
    }

    @Test
    public void testDisabledRule() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("disabled-rule");
        rule.setEnabled(false);
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", Comparator.GT, 0.5));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        RuleEvaluationResult result = ruleEngine.evaluate(rule, highScoreEvent);
        assertFalse("Disabled rule should not trigger", result.isTriggered());
        assertEquals("Rule is disabled", result.getEvaluationDetails());
    }

    @Test
    public void testDifferentComparators() {
        testComparator(Comparator.GT, 0.8, 0.9, true);
        testComparator(Comparator.GT, 0.8, 0.8, false);
        testComparator(Comparator.GTE, 0.8, 0.8, true);
        testComparator(Comparator.LT, 0.8, 0.5, true);
        testComparator(Comparator.LT, 0.8, 0.8, false);
        testComparator(Comparator.LTE, 0.8, 0.8, true);
        testComparator(Comparator.EQ, 0.8, 0.8, true);
        testComparator(Comparator.EQ, 0.8, 0.8001, false);
        testComparator(Comparator.NE, 0.8, 0.9, true);
        testComparator(Comparator.NE, 0.8, 0.8, false);
    }

    private void testComparator(Comparator comparator, double threshold, double score, boolean expected) {
        AlertRule rule = new AlertRule();
        rule.setRuleId("comparator-test-" + comparator);
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream6", comparator, threshold));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        AnomalyScoreEvent event = new AnomalyScoreEvent(System.currentTimeMillis(), score, 0.0, 0.0);
        RuleEvaluationResult result = ruleEngine.evaluate(rule, event);
        assertEquals("Comparator " + comparator + " failed", expected, result.isTriggered());

        ruleEngine.removeRule(rule.getRuleId());
    }

    @Test
    public void testStream11Score() {
        AlertRule rule = new AlertRule();
        rule.setRuleId("stream11-rule");
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition("stream11", Comparator.GT, 0.8));
        rule.setSeverity(Severity.WARNING);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(1);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(1);

        ruleEngine.registerRule(rule);

        AnomalyScoreEvent event = new AnomalyScoreEvent(System.currentTimeMillis(), 0.5, 0.5, 0.9);
        RuleEvaluationResult result = ruleEngine.evaluate(rule, event);
        assertTrue("Stream 11 rule should trigger", result.isTriggered());
    }
}
