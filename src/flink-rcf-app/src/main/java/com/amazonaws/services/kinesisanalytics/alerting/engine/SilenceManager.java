/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.engine;

import com.amazonaws.services.kinesisanalytics.alerting.model.SilenceRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SilenceManager {
    private static final Logger logger = LoggerFactory.getLogger(SilenceManager.class);

    private final Map<String, SilenceRule> silenceRules;
    private final List<SilenceRule> activeRules;

    public SilenceManager() {
        this.silenceRules = new ConcurrentHashMap<>();
        this.activeRules = new CopyOnWriteArrayList<>();
    }

    public void addSilenceRule(SilenceRule rule) {
        if (rule == null || rule.getSilenceId() == null) {
            throw new IllegalArgumentException("Silence rule and id must not be null");
        }
        silenceRules.put(rule.getSilenceId(), rule);
        if (rule.isEnabled()) {
            activeRules.add(rule);
        }
        logger.info("Added silence rule: {}", rule);
    }

    public void removeSilenceRule(String silenceId) {
        SilenceRule rule = silenceRules.remove(silenceId);
        if (rule != null) {
            activeRules.remove(rule);
            logger.info("Removed silence rule: {}", silenceId);
        }
    }

    public void updateSilenceRule(SilenceRule rule) {
        if (rule == null || rule.getSilenceId() == null) {
            throw new IllegalArgumentException("Silence rule and id must not be null");
        }
        SilenceRule existing = silenceRules.get(rule.getSilenceId());
        if (existing != null) {
            activeRules.remove(existing);
        }
        silenceRules.put(rule.getSilenceId(), rule);
        if (rule.isEnabled()) {
            activeRules.add(rule);
        }
        logger.info("Updated silence rule: {}", rule);
    }

    public SilenceRule getSilenceRule(String silenceId) {
        return silenceRules.get(silenceId);
    }

    public List<SilenceRule> getAllSilenceRules() {
        return new ArrayList<>(silenceRules.values());
    }

    public List<SilenceRule> getActiveSilenceRules(long currentTime) {
        List<SilenceRule> result = new ArrayList<>();
        for (SilenceRule rule : activeRules) {
            if (rule.isActive(currentTime)) {
                result.add(rule);
            }
        }
        return result;
    }

    public boolean isSilenced(String ruleId, Map<String, String> dimensions, long currentTime) {
        for (SilenceRule silenceRule : activeRules) {
            if (silenceRule.matches(ruleId, dimensions, currentTime)) {
                logger.debug("Alert silenced by rule: {}", silenceRule.getSilenceId());
                return true;
            }
        }
        return false;
    }

    public List<SilenceRule> getMatchingSilenceRules(String ruleId, Map<String, String> dimensions, long currentTime) {
        List<SilenceRule> result = new ArrayList<>();
        for (SilenceRule silenceRule : activeRules) {
            if (silenceRule.matches(ruleId, dimensions, currentTime)) {
                result.add(silenceRule);
            }
        }
        return result;
    }

    public void cleanupExpiredRules(long currentTime) {
        List<String> expiredIds = new ArrayList<>();
        for (Map.Entry<String, SilenceRule> entry : silenceRules.entrySet()) {
            SilenceRule rule = entry.getValue();
            if (!rule.isEnabled() || currentTime >= rule.getEndTime()) {
                if (currentTime - rule.getEndTime() > 24 * 60 * 60 * 1000) {
                    expiredIds.add(entry.getKey());
                }
            }
        }
        for (String id : expiredIds) {
            removeSilenceRule(id);
        }
    }

    public void clearAllRules() {
        silenceRules.clear();
        activeRules.clear();
        logger.info("Cleared all silence rules");
    }
}
