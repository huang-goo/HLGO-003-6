/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Comparator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Condition;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.EscalationLevel;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Operator;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.Severity;
import com.amazonaws.services.kinesisanalytics.alerting.model.SilenceRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.SilenceRule.MatchType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AlertingConfiguration {

    public static List<AlertRule> createDefaultRules() {
        List<AlertRule> rules = new ArrayList<>();

        AlertRule stream6HighRule = createStreamThresholdRule(
                "rule-stream6-high",
                "Stream 6 High Anomaly",
                "Alert when stream 6 anomaly score exceeds 0.8",
                "stream6",
                Comparator.GT,
                0.8,
                Severity.WARNING
        );
        rules.add(stream6HighRule);

        AlertRule stream9CriticalRule = createStreamThresholdRule(
                "rule-stream9-critical",
                "Stream 9 Critical Anomaly",
                "Alert when stream 9 anomaly score exceeds 0.9",
                "stream9",
                Comparator.GT,
                0.9,
                Severity.CRITICAL
        );
        stream9CriticalRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        stream9CriticalRule.setJitterMinOccurrences(3);
        stream9CriticalRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        stream9CriticalRule.setAutoRecoveryThreshold(5);
        stream9CriticalRule.setEscalationLevels(Arrays.asList(
                new EscalationLevel(1, 5, TimeUnit.MINUTES.toMillis(10), Severity.CRITICAL, "email"),
                new EscalationLevel(2, 10, TimeUnit.MINUTES.toMillis(20), Severity.FATAL, "pager")
        ));
        rules.add(stream9CriticalRule);

        AlertRule combinedRule = new AlertRule();
        combinedRule.setRuleId("rule-combined-anomaly");
        combinedRule.setRuleName("Combined Multi-Stream Anomaly");
        combinedRule.setDescription("Alert when multiple streams show anomalies simultaneously");
        combinedRule.setOperator(Operator.AND);
        combinedRule.addCondition(new Condition("stream6", Comparator.GT, 0.7));
        combinedRule.addCondition(new Condition("stream9", Comparator.GT, 0.7));
        combinedRule.setSeverity(Severity.CRITICAL);
        combinedRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(2));
        combinedRule.setJitterMinOccurrences(2);
        combinedRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(5));
        combinedRule.setAutoRecoveryThreshold(3);
        rules.add(combinedRule);

        AlertRule anyStreamRule = new AlertRule();
        anyStreamRule.setRuleId("rule-any-stream-high");
        anyStreamRule.setRuleName("Any Stream High Anomaly");
        anyStreamRule.setDescription("Alert when any stream shows high anomaly");
        anyStreamRule.setOperator(Operator.OR);
        anyStreamRule.addCondition(new Condition("stream6", Comparator.GT, 0.85));
        anyStreamRule.addCondition(new Condition("stream9", Comparator.GT, 0.85));
        anyStreamRule.addCondition(new Condition("stream11", Comparator.GT, 0.85));
        anyStreamRule.setSeverity(Severity.WARNING);
        anyStreamRule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(3));
        anyStreamRule.setJitterMinOccurrences(2);
        anyStreamRule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(8));
        anyStreamRule.setAutoRecoveryThreshold(4);
        rules.add(anyStreamRule);

        AlertRule stream11Rule = createStreamThresholdRule(
                "rule-stream11-warning",
                "Stream 11 Warning",
                "Alert when stream 11 anomaly score exceeds 0.75",
                "stream11",
                Comparator.GT,
                0.75,
                Severity.WARNING
        );
        rules.add(stream11Rule);

        return rules;
    }

    private static AlertRule createStreamThresholdRule(
            String ruleId, String ruleName, String description,
            String streamName, Comparator comparator, double threshold, Severity severity) {
        AlertRule rule = new AlertRule();
        rule.setRuleId(ruleId);
        rule.setRuleName(ruleName);
        rule.setDescription(description);
        rule.setOperator(Operator.AND);
        rule.addCondition(new Condition(streamName, comparator, threshold));
        rule.setSeverity(severity);
        rule.setJitterSuppressionWindowMs(TimeUnit.MINUTES.toMillis(5));
        rule.setJitterMinOccurrences(2);
        rule.setAutoRecoveryWindowMs(TimeUnit.MINUTES.toMillis(10));
        rule.setAutoRecoveryThreshold(3);
        return rule;
    }

    public static List<SilenceRule> createDefaultSilenceRules() {
        List<SilenceRule> rules = new ArrayList<>();

        long now = System.currentTimeMillis();

        SilenceRule maintenanceSilence = new SilenceRule();
        maintenanceSilence.setName("Scheduled Maintenance");
        maintenanceSilence.setDescription("Silence alerts during scheduled maintenance");
        maintenanceSilence.setCreatedBy("system");
        maintenanceSilence.setStartTime(now);
        maintenanceSilence.setEndTime(now + TimeUnit.HOURS.toMillis(2));
        maintenanceSilence.addDimensionMatcher("env", "test");
        maintenanceSilence.setDimensionMatchType(MatchType.EXACT);
        maintenanceSilence.setComment("Scheduled maintenance window");
        rules.add(maintenanceSilence);

        return rules;
    }

    public static AlertOrchestrator createConfiguredOrchestrator() {
        AlertOrchestrator orchestrator = new AlertOrchestrator();

        for (AlertRule rule : createDefaultRules()) {
            orchestrator.registerRule(rule);
        }

        for (SilenceRule silenceRule : createDefaultSilenceRules()) {
            orchestrator.addSilenceRule(silenceRule);
        }

        return orchestrator;
    }
}
