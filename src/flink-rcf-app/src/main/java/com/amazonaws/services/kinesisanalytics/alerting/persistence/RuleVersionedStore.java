/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.alerting.persistence;

import com.amazonaws.services.kinesisanalytics.alerting.model.AlertRule;

import java.util.List;
import java.util.Optional;

public interface RuleVersionedStore {
    void saveRule(AlertRule rule);

    Optional<AlertRule> getRule(String ruleId);

    Optional<AlertRule> getRuleVersion(String ruleId, long version);

    List<AlertRule> getRuleVersions(String ruleId);

    List<AlertRule> getRuleVersionsByTimeRange(String ruleId, long startTime, long endTime);

    List<AlertRule> getAllRules();

    List<AlertRule> getRulesActiveAtTime(long timestamp);

    Optional<AlertRule> getRuleActiveAtTime(String ruleId, long timestamp);

    void deleteRule(String ruleId);

    void clearAllRules();

    long getRuleCount();

    void close();
}
