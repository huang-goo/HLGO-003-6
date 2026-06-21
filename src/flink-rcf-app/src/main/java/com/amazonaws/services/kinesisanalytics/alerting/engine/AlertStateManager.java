/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.engine;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule.EscalationLevel;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertState;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertState.State;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine.RuleEvaluationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class AlertStateManager {
    private static final Logger logger = LoggerFactory.getLogger(AlertStateManager.class);

    private final Map<String, AlertState> states;
    private final SilenceManager silenceManager;

    public AlertStateManager(SilenceManager silenceManager) {
        this.states = new ConcurrentHashMap<>();
        this.silenceManager = silenceManager;
    }

    public AlertStateManager() {
        this(new SilenceManager());
    }

    public List<AlertEvent> process(RuleEvaluationResult evalResult, AlertRule rule) {
        List<AlertEvent> events = new ArrayList<>();

        String fingerprint = AlertEvent.computeFingerprint(rule.getRuleId(), evalResult.getEvent().getDimensions());
        AlertState state = states.computeIfAbsent(fingerprint,
                k -> new AlertState(fingerprint, rule.getRuleId(), evalResult.getEvent().getDimensions()));

        state.setRuleVersion(rule.getVersion());

        long currentTime = evalResult.getTimestamp();

        if (silenceManager.isSilenced(rule.getRuleId(), state.getDimensions(), currentTime)) {
            return handleSilenced(state, evalResult, rule, currentTime);
        }

        if (evalResult.isTriggered()) {
            events.addAll(handleTriggered(state, evalResult, rule, currentTime));
        } else {
            events.addAll(handleRecovered(state, evalResult, rule, currentTime));
        }

        return events;
    }

    private List<AlertEvent> handleSilenced(AlertState state, RuleEvaluationResult evalResult,
                                            AlertRule rule, long currentTime) {
        List<AlertEvent> events = new ArrayList<>();

        if (state.getState() != State.SILENCED) {
            state.setState(State.SILENCED);
            state.setSilenced(true);
            logger.info("Alert silenced: ruleId={}, fingerprint={}", rule.getRuleId(), state.getFingerprint());
        }

        AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.SILENCED);
        event.setMessage("Alert silenced by rule");
        event.setSilenced(true);
        events.add(event);

        return events;
    }

    private List<AlertEvent> handleTriggered(AlertState state, RuleEvaluationResult evalResult,
                                             AlertRule rule, long currentTime) {
        List<AlertEvent> events = new ArrayList<>();

        state.addTriggerTimestamp(currentTime);
        state.incrementTriggerCount();
        state.resetConsecutiveOkCount();

        long jitterWindowStart = currentTime - rule.getJitterSuppressionWindowMs();
        int triggerCountInWindow = state.getTriggerCountInWindow(jitterWindowStart);

        if (state.getState() == State.OK || state.getState() == State.RECOVERED) {
            if (triggerCountInWindow >= rule.getJitterMinOccurrences()) {
                state.setState(State.FIRING);
                state.setCurrentLevel(0);
                state.setSeverity(rule.getSeverity());

                AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.TRIGGER);
                event.setMessage(buildTriggerMessage(rule, evalResult));
                updateEscalation(state, rule, event, currentTime);
                events.add(event);
                logger.info("Alert triggered: ruleId={}, fingerprint={}, count={}",
                        rule.getRuleId(), state.getFingerprint(), triggerCountInWindow);
            } else if (state.getState() == State.OK) {
                state.setState(State.PENDING);
                logger.debug("Alert pending: ruleId={}, fingerprint={}, count={}/{}",
                        rule.getRuleId(), state.getFingerprint(), triggerCountInWindow, rule.getJitterMinOccurrences());
            }
        } else if (state.getState() == State.PENDING) {
            if (triggerCountInWindow >= rule.getJitterMinOccurrences()) {
                state.setState(State.FIRING);
                state.setCurrentLevel(0);
                state.setSeverity(rule.getSeverity());

                AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.TRIGGER);
                event.setMessage(buildTriggerMessage(rule, evalResult));
                updateEscalation(state, rule, event, currentTime);
                events.add(event);
                logger.info("Alert triggered from pending: ruleId={}, fingerprint={}",
                        rule.getRuleId(), state.getFingerprint());
            }
        } else if (state.getState() == State.FIRING) {
            int previousLevel = state.getCurrentLevel();
            boolean escalated = updateEscalation(state, rule, null, currentTime);
            if (escalated) {
                AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.ESCALATE);
                event.setMessage(buildEscalationMessage(rule, state));
                if (rule.getEscalationLevels() != null) {
                    for (AlertRule.EscalationLevel level : rule.getEscalationLevels()) {
                        if (level.getLevel() == state.getCurrentLevel()) {
                            event.setNotificationChannel(level.getNotificationChannel());
                            break;
                        }
                    }
                }
                events.add(event);
                logger.info("Alert escalated: ruleId={}, fingerprint={}, level={}",
                        rule.getRuleId(), state.getFingerprint(), state.getCurrentLevel());
            } else {
                AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.UPDATE);
                event.setMessage(buildUpdateMessage(rule, evalResult));
                events.add(event);
            }
        }

        return events;
    }

    private List<AlertEvent> handleRecovered(AlertState state, RuleEvaluationResult evalResult,
                                             AlertRule rule, long currentTime) {
        List<AlertEvent> events = new ArrayList<>();

        state.incrementConsecutiveOkCount();
        state.addRecoverTimestamp(currentTime);

        if (state.getState() == State.FIRING || state.getState() == State.PENDING) {
            long recoveryWindowStart = currentTime - rule.getAutoRecoveryWindowMs();
            int recoverCountInWindow = state.getRecoverCountInWindow(recoveryWindowStart);

            if (recoverCountInWindow >= rule.getAutoRecoveryThreshold() &&
                    state.getConsecutiveOkCount() >= rule.getAutoRecoveryThreshold()) {
                state.setState(State.RECOVERED);

                AlertEvent event = createAlertEvent(state, evalResult, rule, AlertEvent.AlertType.RECOVER);
                event.setMessage(buildRecoverMessage(rule, state));
                events.add(event);
                logger.info("Alert recovered: ruleId={}, fingerprint={}, okCount={}",
                        rule.getRuleId(), state.getFingerprint(), state.getConsecutiveOkCount());

                state.reset();
            } else {
                logger.debug("Alert recovering: ruleId={}, fingerprint={}, okCount={}/{}",
                        rule.getRuleId(), state.getFingerprint(),
                        state.getConsecutiveOkCount(), rule.getAutoRecoveryThreshold());
            }
        } else if (state.getState() == State.SILENCED) {
            if (!silenceManager.isSilenced(rule.getRuleId(), state.getDimensions(), currentTime)) {
                state.setState(State.OK);
                state.setSilenced(false);
                logger.info("Alert silence expired: ruleId={}, fingerprint={}",
                        rule.getRuleId(), state.getFingerprint());
            }
        }

        return events;
    }

    private boolean updateEscalation(AlertState state, AlertRule rule, AlertEvent event, long currentTime) {
        if (rule.getEscalationLevels() == null || rule.getEscalationLevels().isEmpty()) {
            return false;
        }

        Optional<EscalationLevel> maxLevel = rule.getEscalationLevels().stream()
                .max(Comparator.comparingInt(EscalationLevel::getLevel));

        int currentLevel = state.getCurrentLevel();
        int triggerCount = state.getTriggerCount();

        for (EscalationLevel level : rule.getEscalationLevels()) {
            if (level.getLevel() > currentLevel && triggerCount >= level.getTriggerCount()) {
                long levelWindowStart = currentTime - level.getTimeWindowMs();
                int countInLevelWindow = state.getTriggerCountInWindow(levelWindowStart);

                if (countInLevelWindow >= level.getTriggerCount()) {
                    state.setCurrentLevel(level.getLevel());
                    state.setSeverity(level.getSeverity());
                    if (event != null) {
                        event.setCurrentLevel(level.getLevel());
                        event.setSeverity(level.getSeverity());
                        event.setNotificationChannel(level.getNotificationChannel());
                    }
                    return true;
                }
            }
        }

        if (event != null) {
            event.setCurrentLevel(state.getCurrentLevel());
            event.setSeverity(state.getSeverity());
            if (maxLevel.isPresent() && maxLevel.get().getLevel() == state.getCurrentLevel()) {
                event.setNotificationChannel(maxLevel.get().getNotificationChannel());
            }
        }

        return false;
    }

    private AlertEvent createAlertEvent(AlertState state, RuleEvaluationResult evalResult,
                                        AlertRule rule, AlertEvent.AlertType type) {
        AlertEvent event = new AlertEvent();
        event.setRuleId(rule.getRuleId());
        event.setRuleVersion(rule.getVersion());
        event.setAlertType(type);
        event.setSeverity(state.getSeverity());
        event.setCurrentLevel(state.getCurrentLevel());
        event.setTimestamp(evalResult.getTimestamp());
        event.setFirstTriggerTimestamp(state.getFirstTriggerTimestamp());
        event.setLastTriggerTimestamp(state.getLastTriggerTimestamp());
        event.setTriggerCount(state.getTriggerCount());
        event.setSourceEvent(evalResult.getEvent());
        event.setDimensions(new ConcurrentHashMap<>(state.getDimensions()));
        event.setFingerprint(state.getFingerprint());
        state.setLastAlertId(event.getAlertId());
        return event;
    }

    private String buildTriggerMessage(AlertRule rule, RuleEvaluationResult result) {
        return String.format("Alert triggered for rule '%s': %s",
                rule.getRuleName() != null ? rule.getRuleName() : rule.getRuleId(),
                result.getEvaluationDetails());
    }

    private String buildEscalationMessage(AlertRule rule, AlertState state) {
        return String.format("Alert escalated to level %d for rule '%s', trigger count: %d",
                state.getCurrentLevel(),
                rule.getRuleName() != null ? rule.getRuleName() : rule.getRuleId(),
                state.getTriggerCount());
    }

    private String buildUpdateMessage(AlertRule rule, RuleEvaluationResult result) {
        return String.format("Alert update for rule '%s': %s",
                rule.getRuleName() != null ? rule.getRuleName() : rule.getRuleId(),
                result.getEvaluationDetails());
    }

    private String buildRecoverMessage(AlertRule rule, AlertState state) {
        return String.format("Alert recovered for rule '%s' after %d triggers, duration: %dms",
                rule.getRuleName() != null ? rule.getRuleName() : rule.getRuleId(),
                state.getTriggerCount(),
                state.getLastTriggerTimestamp() - state.getFirstTriggerTimestamp());
    }

    public AlertState getState(String fingerprint) {
        return states.get(fingerprint);
    }

    public List<AlertState> getAllStates() {
        return new ArrayList<>(states.values());
    }

    public List<AlertState> getFiringStates() {
        List<AlertState> result = new ArrayList<>();
        for (AlertState state : states.values()) {
            if (state.getState() == State.FIRING || state.getState() == State.PENDING) {
                result.add(state);
            }
        }
        return result;
    }

    public void removeState(String fingerprint) {
        states.remove(fingerprint);
    }

    public void clearAllStates() {
        states.clear();
    }

    public SilenceManager getSilenceManager() {
        return silenceManager;
    }
}
