/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting;

import com.amazonaws.services.kinesisanalytics.alerting.engine.AlertStateManager;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine;
import com.amazonaws.services.kinesisanalytics.alerting.engine.RuleEngine.RuleEvaluationResult;
import com.amazonaws.services.kinesisanalytics.alerting.engine.SilenceManager;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;
import com.amazonaws.services.kinesisanalytics.alerting.model.AnomalyScoreEvent;
import com.amazonaws.services.kinesisanalytics.alerting.model.SilenceRule;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.AlertEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.EventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryAlertEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryEventStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.InMemoryRuleVersionedStore;
import com.amazonaws.services.kinesisanalytics.alerting.persistence.RuleVersionedStore;
import com.amazonaws.services.kinesisanalytics.alerting.replay.ReplayEngine;
import com.amazonaws.services.kinesisanalytics.alerting.replay.ReplayEngine.ReplayResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AlertOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(AlertOrchestrator.class);

    private final RuleEngine ruleEngine;
    private final AlertStateManager stateManager;
    private final SilenceManager silenceManager;
    private final EventStore eventStore;
    private final AlertEventStore alertEventStore;
    private final RuleVersionedStore ruleVersionedStore;
    private final ReplayEngine replayEngine;
    private final Map<String, AlertRule> ruleCache;

    private boolean persistenceEnabled = true;

    public AlertOrchestrator() {
        this(new InMemoryEventStore(), new InMemoryAlertEventStore(), new InMemoryRuleVersionedStore());
    }

    public AlertOrchestrator(EventStore eventStore, AlertEventStore alertEventStore,
                             RuleVersionedStore ruleVersionedStore) {
        this.eventStore = eventStore;
        this.alertEventStore = alertEventStore;
        this.ruleVersionedStore = ruleVersionedStore;
        this.silenceManager = new SilenceManager();
        this.ruleEngine = new RuleEngine();
        this.stateManager = new AlertStateManager(silenceManager);
        this.replayEngine = new ReplayEngine(eventStore, alertEventStore, ruleVersionedStore);
        this.ruleCache = new ConcurrentHashMap<>();

        initializeFromStore();
    }

    private void initializeFromStore() {
        List<AlertRule> rules = ruleVersionedStore.getAllRules();
        for (AlertRule rule : rules) {
            ruleEngine.registerRule(rule);
            ruleCache.put(rule.getRuleId(), rule);
        }
        logger.info("Initialized orchestrator with {} rules from store", rules.size());
    }

    public List<AlertEvent> process(AnomalyScoreEvent event) {
        if (event == null) {
            return new ArrayList<>();
        }

        if (persistenceEnabled) {
            eventStore.saveEvent(event);
        }

        List<AlertEvent> allAlertEvents = new ArrayList<>();

        List<RuleEvaluationResult> evalResults = ruleEngine.evaluateAll(event);

        for (RuleEvaluationResult evalResult : evalResults) {
            if (evalResult.hasError()) {
                logger.warn("Rule evaluation error for rule {}: {}",
                        evalResult.getRuleId(), evalResult.getError());
                continue;
            }

            AlertRule rule = ruleCache.get(evalResult.getRuleId());
            if (rule == null) {
                rule = ruleEngine.getRule(evalResult.getRuleId());
                if (rule != null) {
                    ruleCache.put(rule.getRuleId(), rule);
                }
            }

            if (rule != null) {
                List<AlertEvent> alertEvents = stateManager.process(evalResult, rule);
                for (AlertEvent alertEvent : alertEvents) {
                    alertEvent.setFingerprint(
                            AlertEvent.computeFingerprint(rule.getRuleId(), event.getDimensions()));

                    if (persistenceEnabled) {
                        alertEventStore.saveAlertEvent(alertEvent);
                    }

                    allAlertEvents.add(alertEvent);
                    logger.info("Alert generated: type={}, ruleId={}, severity={}, message={}",
                            alertEvent.getAlertType(), alertEvent.getRuleId(),
                            alertEvent.getSeverity(), alertEvent.getMessage());
                }
            }
        }

        return allAlertEvents;
    }

    public void registerRule(AlertRule rule) {
        if (rule == null) {
            throw new IllegalArgumentException("Rule must not be null");
        }

        ruleVersionedStore.saveRule(rule);
        ruleEngine.registerRule(rule);
        ruleCache.put(rule.getRuleId(), rule);

        logger.info("Registered rule: id={}, name={}, version={}",
                rule.getRuleId(), rule.getRuleName(), rule.getVersion());
    }

    public void updateRule(AlertRule rule) {
        registerRule(rule);
    }

    public void removeRule(String ruleId) {
        ruleEngine.removeRule(ruleId);
        ruleVersionedStore.deleteRule(ruleId);
        ruleCache.remove(ruleId);
        logger.info("Removed rule: {}", ruleId);
    }

    public AlertRule getRule(String ruleId) {
        return ruleCache.getOrDefault(ruleId, ruleEngine.getRule(ruleId));
    }

    public List<AlertRule> getAllRules() {
        return ruleEngine.getAllRules();
    }

    public void addSilenceRule(SilenceRule rule) {
        silenceManager.addSilenceRule(rule);
        logger.info("Added silence rule: {}", rule.getSilenceId());
    }

    public void removeSilenceRule(String silenceId) {
        silenceManager.removeSilenceRule(silenceId);
        logger.info("Removed silence rule: {}", silenceId);
    }

    public List<SilenceRule> getAllSilenceRules() {
        return silenceManager.getAllSilenceRules();
    }

    public ReplayResult replay(long startTime, long endTime) {
        return replayEngine.replay(startTime, endTime);
    }

    public ReplayResult replayWithLatestRule(long startTime, long endTime, String ruleId) {
        return replayEngine.replay(startTime, endTime, ruleId, true);
    }

    public ReplayResult replayWithHistoricalRules(long startTime, long endTime, String ruleId) {
        return replayEngine.replay(startTime, endTime, ruleId, false);
    }

    public long getPersistedEventCount() {
        return eventStore.getEventCount();
    }

    public long getPersistedAlertCount() {
        return alertEventStore.getAlertEventCount();
    }

    public List<AnomalyScoreEvent> getEventsByTimeRange(long startTime, long endTime) {
        return eventStore.getEventsByTimeRange(startTime, endTime);
    }

    public List<AlertEvent> getAlertEventsByTimeRange(long startTime, long endTime) {
        return alertEventStore.getAlertEventsByTimeRange(startTime, endTime);
    }

    public void clearAllData() {
        eventStore.clearEvents();
        alertEventStore.clearAlertEvents();
        stateManager.clearAllStates();
        logger.info("Cleared all orchestrator data");
    }

    public void shutdown() {
        eventStore.close();
        alertEventStore.close();
        ruleVersionedStore.close();
        logger.info("Alert orchestrator shutdown complete");
    }

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    public void setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    public RuleEngine getRuleEngine() {
        return ruleEngine;
    }

    public AlertStateManager getStateManager() {
        return stateManager;
    }

    public SilenceManager getSilenceManager() {
        return silenceManager;
    }

    public EventStore getEventStore() {
        return eventStore;
    }

    public AlertEventStore getAlertEventStore() {
        return alertEventStore;
    }

    public RuleVersionedStore getRuleVersionedStore() {
        return ruleVersionedStore;
    }

    public ReplayEngine getReplayEngine() {
        return replayEngine;
    }
}
