/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryRuleVersionedStore implements RuleVersionedStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryRuleVersionedStore.class);

    private final Map<String, TreeMap<Long, AlertRule>> ruleVersions;
    private final Map<String, AlertRule> latestRules;
    private final AtomicLong ruleCount;

    public InMemoryRuleVersionedStore() {
        this.ruleVersions = new ConcurrentHashMap<>();
        this.latestRules = new ConcurrentHashMap<>();
        this.ruleCount = new AtomicLong(0);
    }

    @Override
    public void saveRule(AlertRule rule) {
        if (rule == null || rule.getRuleId() == null) {
            throw new IllegalArgumentException("Rule and ruleId must not be null");
        }

        String ruleId = rule.getRuleId();
        long version = rule.getVersion();

        AlertRule existingLatest = latestRules.get(ruleId);
        if (existingLatest == null) {
            ruleCount.incrementAndGet();
        } else if (existingLatest.equals(rule)) {
            logger.debug("Rule {} unchanged, skipping save", ruleId);
            return;
        } else if (version <= existingLatest.getVersion()) {
            rule.setVersion(existingLatest.getVersion() + 1);
            logger.info("Incremented rule {} version from {} to {}",
                    ruleId, version, rule.getVersion());
            version = rule.getVersion();
        }

        if (rule.getUpdatedAt() == 0) {
            rule.setUpdatedAt(System.currentTimeMillis());
        }

        AlertRule ruleCopy = new AlertRule();
        ruleCopy.setRuleId(rule.getRuleId());
        ruleCopy.setRuleName(rule.getRuleName());
        ruleCopy.setDescription(rule.getDescription());
        ruleCopy.setOperator(rule.getOperator());
        ruleCopy.setConditions(new ArrayList<>(rule.getConditions()));
        ruleCopy.setSeverity(rule.getSeverity());
        ruleCopy.setJitterSuppressionWindowMs(rule.getJitterSuppressionWindowMs());
        ruleCopy.setJitterMinOccurrences(rule.getJitterMinOccurrences());
        ruleCopy.setAutoRecoveryWindowMs(rule.getAutoRecoveryWindowMs());
        ruleCopy.setAutoRecoveryThreshold(rule.getAutoRecoveryThreshold());
        ruleCopy.setEscalationLevels(new ArrayList<>(rule.getEscalationLevels()));
        ruleCopy.setVersion(rule.getVersion());
        ruleCopy.setCreatedAt(rule.getCreatedAt());
        ruleCopy.setUpdatedAt(rule.getUpdatedAt());
        ruleCopy.setEnabled(rule.isEnabled());

        ruleVersions.computeIfAbsent(ruleId, k -> new TreeMap<>())
                .put(version, ruleCopy);

        latestRules.put(ruleId, rule);
        logger.info("Saved rule {} version {}", ruleId, version);
    }

    @Override
    public Optional<AlertRule> getRule(String ruleId) {
        return Optional.ofNullable(latestRules.get(ruleId));
    }

    @Override
    public Optional<AlertRule> getRuleVersion(String ruleId, long version) {
        TreeMap<Long, AlertRule> versions = ruleVersions.get(ruleId);
        if (versions == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(versions.get(version));
    }

    @Override
    public List<AlertRule> getRuleVersions(String ruleId) {
        TreeMap<Long, AlertRule> versions = ruleVersions.get(ruleId);
        if (versions == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(versions.descendingMap().values());
    }

    @Override
    public List<AlertRule> getRuleVersionsByTimeRange(String ruleId, long startTime, long endTime) {
        TreeMap<Long, AlertRule> versions = ruleVersions.get(ruleId);
        List<AlertRule> result = new ArrayList<>();
        if (versions == null) {
            return result;
        }
        for (AlertRule rule : versions.values()) {
            if (rule.getUpdatedAt() >= startTime && rule.getUpdatedAt() <= endTime) {
                result.add(rule);
            }
        }
        result.sort(Comparator.comparingLong(AlertRule::getUpdatedAt).reversed());
        return result;
    }

    @Override
    public List<AlertRule> getAllRules() {
        return new ArrayList<>(latestRules.values());
    }

    @Override
    public List<AlertRule> getRulesActiveAtTime(long timestamp) {
        List<AlertRule> result = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Long, AlertRule>> entry : ruleVersions.entrySet()) {
            TreeMap<Long, AlertRule> versions = entry.getValue();
            AlertRule activeRule = null;
            for (AlertRule rule : versions.values()) {
                if (rule.getUpdatedAt() <= timestamp) {
                    activeRule = rule;
                } else {
                    break;
                }
            }
            if (activeRule != null && activeRule.isEnabled()) {
                result.add(activeRule);
            }
        }
        logger.debug("Found {} rules active at timestamp {}", result.size(), timestamp);
        return result;
    }

    @Override
    public Optional<AlertRule> getRuleActiveAtTime(String ruleId, long timestamp) {
        TreeMap<Long, AlertRule> versions = ruleVersions.get(ruleId);
        if (versions == null) {
            return Optional.empty();
        }
        AlertRule activeRule = null;
        for (AlertRule rule : versions.values()) {
            if (rule.getUpdatedAt() <= timestamp) {
                activeRule = rule;
            } else {
                break;
            }
        }
        if (activeRule != null && activeRule.isEnabled()) {
            return Optional.of(activeRule);
        }
        return Optional.empty();
    }

    @Override
    public void deleteRule(String ruleId) {
        if (latestRules.remove(ruleId) != null) {
            ruleVersions.remove(ruleId);
            ruleCount.decrementAndGet();
            logger.info("Deleted rule {}", ruleId);
        }
    }

    @Override
    public void clearAllRules() {
        ruleVersions.clear();
        latestRules.clear();
        ruleCount.set(0);
        logger.info("Cleared all rules from store");
    }

    @Override
    public long getRuleCount() {
        return ruleCount.get();
    }

    @Override
    public void close() {
        clearAllRules();
        logger.info("Closed rule versioned store");
    }
}
