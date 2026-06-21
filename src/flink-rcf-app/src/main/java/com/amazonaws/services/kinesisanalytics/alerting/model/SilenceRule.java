/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SilenceRule {
    public enum MatchType {
        EXACT, PREFIX, REGEX
    }

    private String silenceId;
    private String name;
    private String description;
    private String createdBy;
    private long createdAt;
    private long startTime;
    private long endTime;
    private Map<String, String> dimensionMatchers;
    private MatchType dimensionMatchType;
    private String ruleIdPattern;
    private MatchType ruleIdMatchType;
    private boolean enabled;
    private String comment;

    public SilenceRule() {
        this.silenceId = UUID.randomUUID().toString();
        this.createdAt = System.currentTimeMillis();
        this.dimensionMatchers = new HashMap<>();
        this.dimensionMatchType = MatchType.EXACT;
        this.ruleIdMatchType = MatchType.EXACT;
        this.enabled = true;
    }

    public String getSilenceId() {
        return silenceId;
    }

    public void setSilenceId(String silenceId) {
        this.silenceId = silenceId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

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

    public Map<String, String> getDimensionMatchers() {
        return dimensionMatchers;
    }

    public void setDimensionMatchers(Map<String, String> dimensionMatchers) {
        this.dimensionMatchers = new HashMap<>(dimensionMatchers);
    }

    public void addDimensionMatcher(String key, String value) {
        this.dimensionMatchers.put(key, value);
    }

    public MatchType getDimensionMatchType() {
        return dimensionMatchType;
    }

    public void setDimensionMatchType(MatchType dimensionMatchType) {
        this.dimensionMatchType = dimensionMatchType;
    }

    public String getRuleIdPattern() {
        return ruleIdPattern;
    }

    public void setRuleIdPattern(String ruleIdPattern) {
        this.ruleIdPattern = ruleIdPattern;
    }

    public MatchType getRuleIdMatchType() {
        return ruleIdMatchType;
    }

    public void setRuleIdMatchType(MatchType ruleIdMatchType) {
        this.ruleIdMatchType = ruleIdMatchType;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isActive(long currentTime) {
        return enabled && currentTime >= startTime && currentTime < endTime;
    }

    public boolean matches(String ruleId, Map<String, String> dimensions, long currentTime) {
        if (!isActive(currentTime)) {
            return false;
        }

        if (ruleIdPattern != null && !ruleIdPattern.isEmpty()) {
            if (!matchesPattern(ruleId, ruleIdPattern, ruleIdMatchType)) {
                return false;
            }
        }

        if (dimensionMatchers != null && !dimensionMatchers.isEmpty()) {
            for (Map.Entry<String, String> matcher : dimensionMatchers.entrySet()) {
                String actualValue = dimensions.get(matcher.getKey());
                if (actualValue == null || !matchesPattern(actualValue, matcher.getValue(), dimensionMatchType)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean matchesPattern(String value, String pattern, MatchType matchType) {
        if (value == null || pattern == null) {
            return false;
        }
        switch (matchType) {
            case EXACT:
                return value.equals(pattern);
            case PREFIX:
                return value.startsWith(pattern);
            case REGEX:
                return value.matches(pattern);
            default:
                return false;
        }
    }

    @Override
    public String toString() {
        return "SilenceRule{" +
                "silenceId='" + silenceId + '\'' +
                ", name='" + name + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", enabled=" + enabled +
                '}';
    }
}
