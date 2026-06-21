/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.model;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class AlertState {
    public enum State {
        OK, PENDING, FIRING, SILENCED, RECOVERED
    }

    private String fingerprint;
    private String ruleId;
    private long ruleVersion;
    private State state;
    private AlertRule.Severity severity;
    private int currentLevel;
    private long firstTriggerTimestamp;
    private long lastTriggerTimestamp;
    private long lastStateChangeTimestamp;
    private int triggerCount;
    private int consecutiveOkCount;
    private Deque<Long> triggerTimestamps;
    private Deque<Long> recoverTimestamps;
    private Map<String, String> dimensions;
    private boolean silenced;
    private String silencedBy;
    private long silencedUntil;
    private String lastAlertId;

    public AlertState() {
        this.state = State.OK;
        this.triggerTimestamps = new ArrayDeque<>();
        this.recoverTimestamps = new ArrayDeque<>();
        this.dimensions = new HashMap<>();
        this.currentLevel = 0;
        this.consecutiveOkCount = 0;
    }

    public AlertState(String fingerprint, String ruleId, Map<String, String> dimensions) {
        this();
        this.fingerprint = fingerprint;
        this.ruleId = ruleId;
        this.dimensions = new HashMap<>(dimensions);
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

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

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
        this.lastStateChangeTimestamp = System.currentTimeMillis();
    }

    public AlertRule.Severity getSeverity() {
        return severity;
    }

    public void setSeverity(AlertRule.Severity severity) {
        this.severity = severity;
    }

    public int getCurrentLevel() {
        return currentLevel;
    }

    public void setCurrentLevel(int currentLevel) {
        this.currentLevel = currentLevel;
    }

    public long getFirstTriggerTimestamp() {
        return firstTriggerTimestamp;
    }

    public void setFirstTriggerTimestamp(long firstTriggerTimestamp) {
        this.firstTriggerTimestamp = firstTriggerTimestamp;
    }

    public long getLastTriggerTimestamp() {
        return lastTriggerTimestamp;
    }

    public void setLastTriggerTimestamp(long lastTriggerTimestamp) {
        this.lastTriggerTimestamp = lastTriggerTimestamp;
    }

    public long getLastStateChangeTimestamp() {
        return lastStateChangeTimestamp;
    }

    public void setLastStateChangeTimestamp(long lastStateChangeTimestamp) {
        this.lastStateChangeTimestamp = lastStateChangeTimestamp;
    }

    public int getTriggerCount() {
        return triggerCount;
    }

    public void setTriggerCount(int triggerCount) {
        this.triggerCount = triggerCount;
    }

    public void incrementTriggerCount() {
        this.triggerCount++;
    }

    public int getConsecutiveOkCount() {
        return consecutiveOkCount;
    }

    public void setConsecutiveOkCount(int consecutiveOkCount) {
        this.consecutiveOkCount = consecutiveOkCount;
    }

    public void incrementConsecutiveOkCount() {
        this.consecutiveOkCount++;
    }

    public void resetConsecutiveOkCount() {
        this.consecutiveOkCount = 0;
    }

    public Deque<Long> getTriggerTimestamps() {
        return triggerTimestamps;
    }

    public void setTriggerTimestamps(Deque<Long> triggerTimestamps) {
        this.triggerTimestamps = triggerTimestamps;
    }

    public void addTriggerTimestamp(long timestamp) {
        this.triggerTimestamps.addLast(timestamp);
        this.lastTriggerTimestamp = timestamp;
        if (this.firstTriggerTimestamp == 0) {
            this.firstTriggerTimestamp = timestamp;
        }
    }

    public void cleanupOldTriggers(long windowStart) {
        while (!triggerTimestamps.isEmpty() && triggerTimestamps.peekFirst() < windowStart) {
            triggerTimestamps.pollFirst();
        }
    }

    public int getTriggerCountInWindow(long windowStart) {
        int count = 0;
        for (long timestamp : triggerTimestamps) {
            if (timestamp >= windowStart) {
                count++;
            }
        }
        return count;
    }

    public Deque<Long> getRecoverTimestamps() {
        return recoverTimestamps;
    }

    public void setRecoverTimestamps(Deque<Long> recoverTimestamps) {
        this.recoverTimestamps = recoverTimestamps;
    }

    public void addRecoverTimestamp(long timestamp) {
        this.recoverTimestamps.addLast(timestamp);
    }

    public void cleanupOldRecovers(long windowStart) {
        while (!recoverTimestamps.isEmpty() && recoverTimestamps.peekFirst() < windowStart) {
            recoverTimestamps.pollFirst();
        }
    }

    public int getRecoverCountInWindow(long windowStart) {
        int count = 0;
        for (long timestamp : recoverTimestamps) {
            if (timestamp >= windowStart) {
                count++;
            }
        }
        return count;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = new HashMap<>(dimensions);
    }

    public boolean isSilenced() {
        return silenced;
    }

    public void setSilenced(boolean silenced) {
        this.silenced = silenced;
    }

    public String getSilencedBy() {
        return silencedBy;
    }

    public void setSilencedBy(String silencedBy) {
        this.silencedBy = silencedBy;
    }

    public long getSilencedUntil() {
        return silencedUntil;
    }

    public void setSilencedUntil(long silencedUntil) {
        this.silencedUntil = silencedUntil;
    }

    public boolean isSilenceActive(long currentTime) {
        return silenced && currentTime < silencedUntil;
    }

    public String getLastAlertId() {
        return lastAlertId;
    }

    public void setLastAlertId(String lastAlertId) {
        this.lastAlertId = lastAlertId;
    }

    public void reset() {
        this.state = State.OK;
        this.severity = null;
        this.currentLevel = 0;
        this.firstTriggerTimestamp = 0;
        this.lastTriggerTimestamp = 0;
        this.lastStateChangeTimestamp = System.currentTimeMillis();
        this.triggerCount = 0;
        this.consecutiveOkCount = 0;
        this.triggerTimestamps.clear();
        this.recoverTimestamps.clear();
        this.silenced = false;
        this.silencedBy = null;
        this.silencedUntil = 0;
        this.lastAlertId = null;
    }

    @Override
    public String toString() {
        return "AlertState{" +
                "fingerprint='" + fingerprint + '\'' +
                ", ruleId='" + ruleId + '\'' +
                ", state=" + state +
                ", severity=" + severity +
                ", currentLevel=" + currentLevel +
                ", triggerCount=" + triggerCount +
                '}';
    }
}
