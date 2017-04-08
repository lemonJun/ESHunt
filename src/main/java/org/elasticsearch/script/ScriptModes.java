/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.*;

/**
 * Holds the {@link org.elasticsearch.script.ScriptMode}s for each of the different scripting languages available,
 * each script source and each scripted operation.
 */
public class ScriptModes {

    static final String SCRIPT_SETTINGS_PREFIX = "script.";
    static final String ENGINE_SETTINGS_PREFIX = "script.engine";

    final ImmutableMap<String, ScriptMode> scriptModes;

    ScriptModes(Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, Settings settings, ESLogger logger) {
        //filter out the native engine as we don't want to apply fine grained settings to it.
        //native scripts are always on as they are static by definition.
        Map<String, ScriptEngineService> filteredEngines = Maps.newHashMap(scriptEngines);
        filteredEngines.remove(NativeScriptEngineService.NAME);
        this.scriptModes = buildScriptModeSettingsMap(settings, filteredEngines, scriptContextRegistry, logger);
    }

    private static ImmutableMap<String, ScriptMode> buildScriptModeSettingsMap(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, ESLogger logger) {
        HashMap<String, ScriptMode> scriptModesMap = Maps.newHashMap();

        //file scripts are enabled by default, for any language
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.FILE, ScriptMode.ON, scriptModesMap);
        //indexed scripts are enabled by default only for sandboxed languages
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INDEXED, ScriptMode.SANDBOX, scriptModesMap);
        //dynamic scripts are enabled by default only for sandboxed languages
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INLINE, ScriptMode.SANDBOX, scriptModesMap);

        List<String> processedSettings = Lists.newArrayList();
        processSourceBasedGlobalSettings(settings, scriptEngines, scriptContextRegistry, processedSettings, scriptModesMap);
        processOperationBasedGlobalSettings(settings, scriptEngines, scriptContextRegistry, processedSettings, scriptModesMap);
        processEngineSpecificSettings(settings, scriptEngines, scriptContextRegistry, processedSettings, scriptModesMap);
        processDisableDynamicDeprecatedSetting(settings, scriptEngines, scriptContextRegistry, processedSettings, scriptModesMap, logger);
        return ImmutableMap.copyOf(scriptModesMap);
    }

    private static void processSourceBasedGlobalSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, List<String> processedSettings, Map<String, ScriptMode> scriptModes) {
        //read custom source based settings for all operations (e.g. script.indexed: on)
        for (ScriptType scriptType : ScriptType.values()) {
            String scriptTypeSetting = settings.get(SCRIPT_SETTINGS_PREFIX + scriptType);
            if (Strings.hasLength(scriptTypeSetting)) {
                ScriptMode scriptTypeMode = ScriptMode.parse(scriptTypeSetting);
                processedSettings.add(SCRIPT_SETTINGS_PREFIX + scriptType + ": " + scriptTypeMode);
                addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, scriptType, scriptTypeMode, scriptModes);
            }
        }
    }

    private static void processOperationBasedGlobalSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, List<String> processedSettings, Map<String, ScriptMode> scriptModes) {
        //read custom op based settings for all sources (e.g. script.aggs: off)
        //op based settings take precedence over source based settings, hence they get expanded later
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            ScriptMode scriptMode = getScriptContextMode(settings, SCRIPT_SETTINGS_PREFIX, scriptContext);
            if (scriptMode != null) {
                processedSettings.add(SCRIPT_SETTINGS_PREFIX + scriptContext.getKey() + ": " + scriptMode);
                addGlobalScriptContextModes(scriptEngines.keySet(), scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void processEngineSpecificSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, List<String> processedSettings, Map<String, ScriptMode> scriptModes) {
        Map<String, Settings> langGroupedSettings = settings.getGroups(ENGINE_SETTINGS_PREFIX, true);
        for (Map.Entry<String, Settings> langSettings : langGroupedSettings.entrySet()) {
            //read engine specific settings that refer to a non existing script lang will be ignored
            ScriptEngineService scriptEngineService = scriptEngines.get(langSettings.getKey());
            if (scriptEngineService != null) {
                String enginePrefix = ScriptModes.ENGINE_SETTINGS_PREFIX + "." + langSettings.getKey() + ".";
                for (ScriptType scriptType : ScriptType.values()) {
                    String scriptTypePrefix = scriptType + ".";
                    for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                        ScriptMode scriptMode = getScriptContextMode(langSettings.getValue(), scriptTypePrefix, scriptContext);
                        if (scriptMode != null) {
                            processedSettings.add(enginePrefix + scriptTypePrefix + scriptContext.getKey() + ": " + scriptMode);
                            addScriptMode(scriptEngineService, scriptType, scriptContext, scriptMode, scriptModes);
                        }
                    }
                }
            }
        }
    }

    private static void processDisableDynamicDeprecatedSetting(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, List<String> processedSettings, Map<String, ScriptMode> scriptModes, ESLogger logger) {
        //read deprecated disable_dynamic setting, apply only if none of the new settings is used
        String disableDynamicSetting = settings.get(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING);
        if (disableDynamicSetting != null) {
            if (processedSettings.isEmpty()) {
                ScriptService.DynamicScriptDisabling dynamicScriptDisabling = ScriptService.DynamicScriptDisabling.parse(disableDynamicSetting);
                switch (dynamicScriptDisabling) {
                    case EVERYTHING_ALLOWED:
                        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INDEXED, ScriptMode.ON, scriptModes);
                        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INLINE, ScriptMode.ON, scriptModes);
                        break;
                    case ONLY_DISK_ALLOWED:
                        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INDEXED, ScriptMode.OFF, scriptModes);
                        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INLINE, ScriptMode.OFF, scriptModes);
                        break;
                }
                logger.warn("deprecated setting [{}] is set, replace with fine-grained scripting settings (e.g. script.inline, script.indexed, script.file)", ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING);
            } else {
                processedSettings.add(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING + ": " + disableDynamicSetting);
                throw new ElasticsearchIllegalArgumentException("conflicting scripting settings have been specified, use either " + ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING + " (deprecated) or the newer fine-grained settings (e.g. script.inline, script.indexed, script.file), not both at the same time:\n" + processedSettings);
            }
        }
    }

    private static ScriptMode getScriptContextMode(Settings settings, String prefix, ScriptContext scriptContext) {
        String settingValue = settings.get(prefix + scriptContext.getKey());
        if (Strings.hasLength(settingValue)) {
            return ScriptMode.parse(settingValue);
        }
        return null;
    }

    private static void addGlobalScriptTypeModes(Set<String> langs, ScriptContextRegistry scriptContextRegistry, ScriptType scriptType, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                addScriptMode(lang, scriptType, scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void addGlobalScriptContextModes(Set<String> langs, ScriptContext scriptContext, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptType scriptType : ScriptType.values()) {
                addScriptMode(lang, scriptType, scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void addScriptMode(ScriptEngineService scriptEngineService, ScriptType scriptType, ScriptContext scriptContext, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        //expand the lang specific settings to all of the different names given to each scripting language
        for (String scriptEngineName : scriptEngineService.types()) {
            addScriptMode(scriptEngineName, scriptType, scriptContext, scriptMode, scriptModes);
        }
    }

    private static void addScriptMode(String lang, ScriptType scriptType, ScriptContext scriptContext, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        scriptModes.put(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey(), scriptMode);
    }

    /**
     * Returns the script mode for a script of a certain written in a certain language,
     * of a certain type and executing as part of a specific operation/api.
     *
     * @param lang the language that the script is written in
     * @param scriptType the type of the script
     * @param scriptContext the operation that requires the execution of the script
     * @return whether scripts are on, off, or enabled only for sandboxed languages
     */
    public ScriptMode getScriptMode(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        //native scripts are always on as they are static by definition
        if (NativeScriptEngineService.NAME.equals(lang)) {
            return ScriptMode.ON;
        }
        ScriptMode scriptMode = scriptModes.get(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey());
        if (scriptMode == null) {
            throw new ElasticsearchIllegalArgumentException("script mode not found for lang [" + lang + "], script_type [" + scriptType + "], operation [" + scriptContext.getKey() + "]");
        }
        return scriptMode;
    }

    @Override
    public String toString() {
        //order settings by key before printing them out, for readability
        TreeMap<String, ScriptMode> scriptModesTreeMap = new TreeMap<>();
        scriptModesTreeMap.putAll(scriptModes);
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ScriptMode> stringScriptModeEntry : scriptModesTreeMap.entrySet()) {
            stringBuilder.append(stringScriptModeEntry.getKey()).append(": ").append(stringScriptModeEntry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
