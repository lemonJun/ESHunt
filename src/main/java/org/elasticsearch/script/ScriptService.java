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

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.*;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptService extends AbstractComponent implements Closeable {

    public static final String DEFAULT_SCRIPTING_LANGUAGE_SETTING = "script.default_lang";
    public static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";
    public static final String SCRIPT_CACHE_SIZE_SETTING = "script.cache.max_size";
    public static final String SCRIPT_CACHE_EXPIRE_SETTING = "script.cache.expire";
    public static final String SCRIPT_INDEX = ".scripts";
    public static final String DEFAULT_LANG = GroovyScriptEngineService.NAME;

    private final String defaultLang;

    private final Set<ScriptEngineService> scriptEngines;
    private final ImmutableMap<String, ScriptEngineService> scriptEnginesByLang;
    private final ImmutableMap<String, ScriptEngineService> scriptEnginesByExt;

    private final ConcurrentMap<CacheKey, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<CacheKey, CompiledScript> cache;
    private final File scriptsDirectory;
    private final FileWatcher fileWatcher;

    private final ScriptModes scriptModes;
    private final ScriptContextRegistry scriptContextRegistry;

    private Client client = null;

    /**
     * Enum defining the different dynamic settings for scripting, either
     * ONLY_DISK_ALLOWED (scripts must be placed on disk), EVERYTHING_ALLOWED
     * (all dynamic scripting is enabled), or SANDBOXED_ONLY (only sandboxed
     * scripting languages are allowed)
     */
    enum DynamicScriptDisabling {
        EVERYTHING_ALLOWED, ONLY_DISK_ALLOWED, SANDBOXED_ONLY;

        static DynamicScriptDisabling parse(String s) {
            switch (s.toLowerCase(Locale.ROOT)) {
                // true for "disable_dynamic" means only on-disk scripts are enabled
                case "true":
                case "all":
                    return ONLY_DISK_ALLOWED;
                // false for "disable_dynamic" means all scripts are enabled
                case "false":
                case "none":
                    return EVERYTHING_ALLOWED;
                // only sandboxed scripting is enabled
                case "sandbox":
                case "sandboxed":
                    return SANDBOXED_ONLY;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unrecognized script allowance setting: [" + s + "]");
            }
        }
    }

    public static final ParseField SCRIPT_LANG = new ParseField("lang", "script_lang");
    public static final ParseField SCRIPT_FILE = new ParseField("script_file", "file");
    public static final ParseField SCRIPT_ID = new ParseField("script_id", "id");
    public static final ParseField SCRIPT_INLINE = new ParseField("script", "scriptField");
    public static final ParseField VALUE_SCRIPT_FILE = new ParseField("value_script_file");
    public static final ParseField VALUE_SCRIPT_ID = new ParseField("value_script_id");
    public static final ParseField VALUE_SCRIPT_INLINE = new ParseField("value_script");
    public static final ParseField KEY_SCRIPT_FILE = new ParseField("key_script_file");
    public static final ParseField KEY_SCRIPT_ID = new ParseField("key_script_id");
    public static final ParseField KEY_SCRIPT_INLINE = new ParseField("key_script");

    @Inject
    public ScriptService(Settings settings, Environment env, Set<ScriptEngineService> scriptEngines, ResourceWatcherService resourceWatcherService, NodeSettingsService nodeSettingsService, ScriptContextRegistry scriptContextRegistry) throws IOException {
        super(settings);

        this.scriptEngines = scriptEngines;
        this.scriptContextRegistry = scriptContextRegistry;
        int cacheMaxSize = settings.getAsInt(SCRIPT_CACHE_SIZE_SETTING, 100);
        TimeValue cacheExpire = settings.getAsTime(SCRIPT_CACHE_EXPIRE_SETTING, null);
        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);

        this.defaultLang = settings.get(DEFAULT_SCRIPTING_LANGUAGE_SETTING, DEFAULT_LANG);

        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.maximumSize(cacheMaxSize);
        }
        if (cacheExpire != null) {
            cacheBuilder.expireAfterAccess(cacheExpire.nanos(), TimeUnit.NANOSECONDS);
        }
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        ImmutableMap.Builder<String, ScriptEngineService> enginesByLangBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, ScriptEngineService> enginesByExtBuilder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                enginesByLangBuilder.put(type, scriptEngine);
            }
            for (String ext : scriptEngine.extensions()) {
                enginesByExtBuilder.put(ext, scriptEngine);
            }
        }
        this.scriptEnginesByLang = enginesByLangBuilder.build();
        this.scriptEnginesByExt = enginesByExtBuilder.build();

        this.scriptModes = new ScriptModes(this.scriptEnginesByLang, scriptContextRegistry, settings, logger);

        // add file watcher for static scripts
        scriptsDirectory = new File(env.configFile(), "scripts");
        if (logger.isTraceEnabled()) {
            logger.trace("Using scripts directory [{}] ", scriptsDirectory);
        }
        this.fileWatcher = new FileWatcher(scriptsDirectory);
        fileWatcher.addListener(new ScriptChangesListener());

        if (componentSettings.getAsBoolean("auto_reload_enabled", true)) {
            // automatic reload is enabled - register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disable just load scripts once
            fileWatcher.init();
        }
        nodeSettingsService.addListener(new ApplySettings());
    }

    //This isn't set in the ctor because doing so creates a guice circular
    @Inject(optional = true)
    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(scriptEngines);
    }

    /**
     * Clear both the in memory and on disk compiled script caches. Files on
     * disk will be treated as if they are new and recompiled.
     * */
    public void clearCache() {
        logger.debug("clearing script cache");
        // Clear the in-memory script caches
        this.cache.invalidateAll();
        this.cache.cleanUp();
        // Clear the cache of on-disk scripts
        this.staticCache.clear();
        // Clear the file watcher's state so it re-compiles on-disk scripts
        this.fileWatcher.clearState();
    }

    private ScriptEngineService getScriptEngineServiceForLang(String lang) {
        ScriptEngineService scriptEngineService = scriptEnginesByLang.get(lang);
        if (scriptEngineService == null) {
            throw new ElasticsearchIllegalArgumentException("script_lang not supported [" + lang + "]");
        }
        return scriptEngineService;
    }

    private ScriptEngineService getScriptEngineServiceForFileExt(String fileExtension) {
        ScriptEngineService scriptEngineService = scriptEnginesByExt.get(fileExtension);
        if (scriptEngineService == null) {
            throw new ElasticsearchIllegalArgumentException("script file extension not supported [" + fileExtension + "]");
        }
        return scriptEngineService;
    }

    /**
     * Checks if a script can be executed and compiles it if needed, or returns the previously compiled and cached script.
     * Doesn't require to specify a script context in order to maintain backwards compatibility, internally uses
     * the {@link org.elasticsearch.script.ScriptContext.Standard#GENERIC_PLUGIN} default context, assuming that it can only be called from plugins.
     *
     * @deprecated use the method variant that accepts the {@link ScriptContext} argument too: {@link #compile(String, String, ScriptType, ScriptContext)}
     */
    @Deprecated
    public CompiledScript compile(String lang, String scriptOrId, ScriptType scriptType) {
        return compile(lang, scriptOrId, scriptType, ScriptContext.Standard.GENERIC_PLUGIN);
    }

    /**
     * Checks if a script can be executed and compiles it if needed, or returns the previously compiled and cached script.
     */
    public CompiledScript compile(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        assert scriptContext != null;

        //scriptType might not get serialized depending on the version of the node we talk to, if null treat as inline
        if (scriptType == null) {
            scriptType = ScriptType.INLINE;
        }
        if (lang == null) {
            lang = defaultLang;
        }

        ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(lang);
        //For backwards compat attempt to load from disk
        if (scriptType == ScriptType.INLINE) {
            CacheKey cacheKey = newCacheKey(scriptEngineService, script);
            CompiledScript compiled = staticCache.get(cacheKey); //On disk scripts will be loaded into the staticCache by the listener
            if (compiled != null) {
                scriptType = ScriptType.FILE;
                if (canExecuteScript(lang, scriptEngineService, scriptType, scriptContext) == false) {
                    throw new ScriptException("scripts of type [" + scriptType + "], operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are disabled");
                }
                return compiled;
            }
        }

        if (canExecuteScript(lang, scriptEngineService, scriptType, scriptContext) == false) {
            throw new ScriptException("scripts of type [" + scriptType + "], operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are disabled");
        }
        return compileInternal(lang, script, scriptType);
    }

    /**
     * Compiles a script straight-away, or returns the previously compiled and cached script, without checking if it can be executed based on settings.
     */
    public CompiledScript compileInternal(String lang, String scriptOrId, ScriptType scriptType) {
        assert scriptType != null;
        if (lang == null) {
            lang = defaultLang;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Compiling lang: [{}] type: [{}] script: {}", lang, scriptType, scriptOrId);
        }

        ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(lang);
        CacheKey cacheKey = newCacheKey(scriptEngineService, scriptOrId);

        if (scriptType == ScriptType.FILE) {
            CompiledScript compiled = staticCache.get(cacheKey); //On disk scripts will be loaded into the staticCache by the listener
            if (compiled == null) {
                throw new ElasticsearchIllegalArgumentException("Unable to find on disk script " + scriptOrId);
            }
            return compiled;
        }

        String script = scriptOrId;
        if (scriptType == ScriptType.INDEXED) {
            final IndexedScript indexedScript = new IndexedScript(lang, scriptOrId);
            script = getScriptFromIndex(indexedScript.lang, indexedScript.id);
            cacheKey = newCacheKey(scriptEngineService, script);
        }

        CompiledScript compiled = cache.getIfPresent(cacheKey);
        if (compiled == null) {
            //Either an un-cached inline script or an indexed script
            compiled = new CompiledScript(lang, scriptEngineService.compile(script));
            //Since the cache key is the script content itself we don't need to
            //invalidate/check the cache if an indexed script changes.
            cache.put(cacheKey, compiled);
        }
        return compiled;
    }

    public void queryScriptIndex(GetIndexedScriptRequest request, final ActionListener<GetResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        GetRequest getRequest = new GetRequest(request, SCRIPT_INDEX).type(scriptLang).id(request.id()).version(request.version()).versionType(request.versionType()).preference("_local"); //Set preference for no forking
        client.get(getRequest, listener);
    }

    private String validateScriptLanguage(String scriptLang) {
        if (scriptLang == null) {
            scriptLang = defaultLang;
        } else if (scriptEnginesByLang.containsKey(scriptLang) == false) {
            throw new ElasticsearchIllegalArgumentException("script_lang not supported [" + scriptLang + "]");
        }
        return scriptLang;
    }

    String getScriptFromIndex(String scriptLang, String id) {
        if (client == null) {
            throw new ElasticsearchIllegalArgumentException("Got an indexed script with no Client registered.");
        }
        scriptLang = validateScriptLanguage(scriptLang);
        GetRequest getRequest = new GetRequest(SCRIPT_INDEX, scriptLang, id);
        getRequest.copyContextAndHeadersFrom(SearchContext.current());
        GetResponse responseFields = client.get(getRequest).actionGet();
        if (responseFields.isExists()) {
            return getScriptFromResponse(responseFields);
        }
        throw new ElasticsearchIllegalArgumentException("Unable to find script [" + SCRIPT_INDEX + "/" + scriptLang + "/" + id + "]");
    }

    private void validate(BytesReference scriptBytes, String scriptLang) {
        try {
            XContentParser parser = XContentFactory.xContent(scriptBytes).createParser(scriptBytes);
            TemplateQueryParser.TemplateContext context = TemplateQueryParser.parse(parser, "params", "script", "template");
            if (Strings.hasLength(context.template())) {
                //Just try and compile it
                //This will have the benefit of also adding the script to the cache if it compiles
                try {
                    //we don't know yet what the script will be used for, but if all of the operations for this lang with
                    //indexed scripts are disabled, it makes no sense to even compile it and cache it.
                    if (isAnyScriptContextEnabled(scriptLang, getScriptEngineServiceForLang(scriptLang), ScriptType.INDEXED)) {
                        CompiledScript compiledScript = compileInternal(scriptLang, context.template(), ScriptType.INLINE);
                        if (compiledScript == null) {
                            throw new ElasticsearchIllegalArgumentException("Unable to parse [" + context.template() + "] lang [" + scriptLang + "] (ScriptService.compile returned null)");
                        }
                    } else {
                        logger.warn("skipping compile of script [{}], lang [{}] as all scripted operations are disabled for indexed scripts", context.template(), scriptLang);
                    }
                } catch (Exception e) {
                    throw new ElasticsearchIllegalArgumentException("Unable to parse [" + context.template() + "] lang [" + scriptLang + "]", e);
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unable to find script in : " + scriptBytes.toUtf8());
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template script", e);
        }
    }

    public void putScriptToIndex(PutIndexedScriptRequest request, ActionListener<IndexResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        //verify that the script compiles
        validate(request.source(), scriptLang);

        IndexRequest indexRequest = new IndexRequest(request).index(SCRIPT_INDEX).type(scriptLang).id(request.id()).version(request.version()).versionType(request.versionType()).source(request.source()).opType(request.opType()).refresh(true); //Always refresh after indexing a template
        client.index(indexRequest, listener);
    }

    public void deleteScriptFromIndex(DeleteIndexedScriptRequest request, ActionListener<DeleteResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        DeleteRequest deleteRequest = new DeleteRequest(request).index(SCRIPT_INDEX).type(scriptLang).id(request.id()).refresh(true).version(request.version()).versionType(request.versionType());
        client.delete(deleteRequest, listener);
    }

    @SuppressWarnings("unchecked")
    public static String getScriptFromResponse(GetResponse responseFields) {
        Map<String, Object> source = responseFields.getSourceAsMap();
        if (source.containsKey("template")) {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                Object template = source.get("template");
                if (template instanceof Map) {
                    builder.map((Map<String, Object>) template);
                    return builder.string();
                } else {
                    return template.toString();
                }
            } catch (IOException | ClassCastException e) {
                throw new ElasticsearchIllegalStateException("Unable to parse " + responseFields.getSourceAsString() + " as json", e);
            }
        } else if (source.containsKey("script")) {
            return source.get("script").toString();
        } else {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(responseFields.getSource());
                return builder.string();
            } catch (IOException | ClassCastException e) {
                throw new ElasticsearchIllegalStateException("Unable to parse " + responseFields.getSourceAsString() + " as json", e);
            }
        }
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided script.
     * Doesn't require to specify a script context in order to maintain backwards compatibility, internally uses
     * the {@link org.elasticsearch.script.ScriptContext.Standard#GENERIC_PLUGIN} default context, assuming that it can only be called from plugins.
     *
     * @deprecated use the method variant that accepts the {@link ScriptContext} argument too: {@link #executable(String, String, ScriptType, ScriptContext, Map)}
     */
    @Deprecated
    public ExecutableScript executable(String lang, String script, ScriptType scriptType, Map<String, Object> vars) {
        return executable(lang, script, scriptType, ScriptContext.Standard.GENERIC_PLUGIN, vars);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided script
     */
    public ExecutableScript executable(String lang, String script, ScriptType scriptType, ScriptContext scriptContext, Map<String, Object> vars) {
        return executable(compile(lang, script, scriptType, scriptContext), vars);
    }

    /**
     * Executes a previously compiled script provided as an argument
     */
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return getScriptEngineServiceForLang(compiledScript.lang()).executable(compiledScript.compiled(), vars);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided search script
     * Doesn't require to specify a script context in order to maintain backwards compatibility, internally uses
     * the {@link org.elasticsearch.script.ScriptContext.Standard#GENERIC_PLUGIN} default context, assuming that it can only be called from plugins.
     *
     * @deprecated use the method variant that accepts the {@link ScriptContext} argument too: {@link #search(SearchLookup, String, String, ScriptType, ScriptContext, Map)}
     */
    @Deprecated
    public SearchScript search(SearchLookup lookup, String lang, String script, ScriptType scriptType, @Nullable Map<String, Object> vars) {
        return search(lookup, lang, script, scriptType, ScriptContext.Standard.GENERIC_PLUGIN, vars);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided search script
     */
    public SearchScript search(SearchLookup lookup, String lang, String script, ScriptType scriptType, ScriptContext scriptContext, @Nullable Map<String, Object> vars) {
        CompiledScript compiledScript = compile(lang, script, scriptType, scriptContext);
        return getScriptEngineServiceForLang(compiledScript.lang()).search(compiledScript.compiled(), lookup, vars);
    }

    private boolean isAnyScriptContextEnabled(String lang, ScriptEngineService scriptEngineService, ScriptType scriptType) {
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            if (canExecuteScript(lang, scriptEngineService, scriptType, scriptContext)) {
                return true;
            }
        }
        return false;
    }

    private boolean canExecuteScript(String lang, ScriptEngineService scriptEngineService, ScriptType scriptType, ScriptContext scriptContext) {
        assert lang != null;
        if (scriptContextRegistry.isSupportedContext(scriptContext) == false) {
            throw new ElasticsearchIllegalArgumentException("script context [" + scriptContext.getKey() + "] not supported");
        }
        ScriptMode mode = scriptModes.getScriptMode(lang, scriptType, scriptContext);
        switch (mode) {
            case ON:
                return true;
            case OFF:
                return false;
            case SANDBOX:
                return scriptEngineService.sandboxed();
            default:
                throw new ElasticsearchIllegalArgumentException("script mode [" + mode + "] not supported");
        }
    }

    /**
     * A small listener for the script cache that calls each
     * {@code ScriptEngineService}'s {@code scriptRemoved} method when the
     * script has been removed from the cache
     */
    private class ScriptCacheRemovalListener implements RemovalListener<CacheKey, CompiledScript> {

        @Override
        public void onRemoval(RemovalNotification<CacheKey, CompiledScript> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug("notifying script services of script removal due to: [{}]", notification.getCause());
            }
            for (ScriptEngineService service : scriptEngines) {
                try {
                    service.scriptRemoved(notification.getValue());
                } catch (Exception e) {
                    logger.warn("exception calling script removal listener for script service", e);
                    // We don't rethrow because Guava would just catch the
                    // exception and log it, which we have already done
                }
            }
        }
    }

    private class ScriptChangesListener extends FileChangesListener {

        private Tuple<String, String> scriptNameExt(File file) {
            String scriptPath = scriptsDirectory.toURI().relativize(file.toURI()).getPath();
            int extIndex = scriptPath.lastIndexOf('.');
            if (extIndex != -1) {
                String ext = scriptPath.substring(extIndex + 1);
                String scriptName = scriptPath.substring(0, extIndex).replace(File.separatorChar, '_');
                return new Tuple<>(scriptName, ext);
            } else {
                return null;
            }
        }

        @Override
        public void onFileInit(File file) {
            if (logger.isTraceEnabled()) {
                logger.trace("Loading script file : [{}]", file);
            }
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
                if (engineService == null) {
                    logger.warn("no script engine found for [{}]", scriptNameExt.v2());
                } else {
                    try {
                        //we don't know yet what the script will be used for, but if all of the operations for this lang
                        // with file scripts are disabled, it makes no sense to even compile it and cache it.
                        if (isAnyScriptContextEnabled(engineService.types()[0], engineService, ScriptType.FILE)) {
                            logger.info("compiling script file [{}]", file.getAbsolutePath());
                            try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8)) {
                                String script = Streams.copyToString(reader);
                                CacheKey cacheKey = newCacheKey(engineService, scriptNameExt.v1());
                                staticCache.put(cacheKey, new CompiledScript(cacheKey.lang, engineService.compile(script)));
                            }
                        } else {
                            logger.warn("skipping compile of script file [{}] as all scripted operations are disabled for file scripts", file.getAbsolutePath());
                        }
                    } catch (Throwable e) {
                        logger.warn("failed to load/compile script [{}]", e, scriptNameExt.v1());
                    }
                }
            }
        }

        @Override
        public void onFileCreated(File file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(File file) {
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
                assert engineService != null;
                logger.info("removing script file [{}]", file.getAbsolutePath());
                staticCache.remove(newCacheKey(engineService, scriptNameExt.v1()));
            }
        }

        @Override
        public void onFileChanged(File file) {
            onFileInit(file);
        }

    }

    /**
     * The type of a script, more specifically where it gets loaded from:
     * - provided dynamically at request time
     * - loaded from an index
     * - loaded from file
     */
    public static enum ScriptType {

        INLINE, INDEXED, FILE;

        private static final int INLINE_VAL = 0;
        private static final int INDEXED_VAL = 1;
        private static final int FILE_VAL = 2;

        public static ScriptType readFrom(StreamInput in) throws IOException {
            int scriptTypeVal = in.readVInt();
            switch (scriptTypeVal) {
                case INDEXED_VAL:
                    return INDEXED;
                case INLINE_VAL:
                    return INLINE;
                case FILE_VAL:
                    return FILE;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unexpected value read for ScriptType got [" + scriptTypeVal + "] expected one of [" + INLINE_VAL + "," + INDEXED_VAL + "," + FILE_VAL + "]");
            }
        }

        public static void writeTo(ScriptType scriptType, StreamOutput out) throws IOException {
            if (scriptType != null) {
                switch (scriptType) {
                    case INDEXED:
                        out.writeVInt(INDEXED_VAL);
                        return;
                    case INLINE:
                        out.writeVInt(INLINE_VAL);
                        return;
                    case FILE:
                        out.writeVInt(FILE_VAL);
                        return;
                    default:
                        throw new ElasticsearchIllegalStateException("Unknown ScriptType " + scriptType);
                }
            } else {
                out.writeVInt(INLINE_VAL); //Default to inline
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static CacheKey newCacheKey(ScriptEngineService engineService, String script) {
        return new CacheKey(engineService.types()[0], script);
    }

    private static class CacheKey {
        public final String lang;
        public final String script;

        public CacheKey(String lang, String script) {
            this.lang = lang;
            this.script = script;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey other = (CacheKey) o;
            return lang.equals(other.lang) && script.equals(other.script);
        }

        @Override
        public int hashCode() {
            return lang.hashCode() + 31 * script.hashCode();
        }
    }

    private static class IndexedScript {
        private final String lang;
        private final String id;

        IndexedScript(String lang, String script) {
            this.lang = lang;
            final String[] parts = script.split("/");
            if (parts.length == 1) {
                this.id = script;
            } else {
                if (parts.length != 3) {
                    throw new ElasticsearchIllegalArgumentException("Illegal index script format [" + script + "]" + " should be /lang/id");
                } else {
                    if (!parts[1].equals(this.lang)) {
                        throw new ElasticsearchIllegalStateException("Conflicting script language, found [" + parts[1] + "] expected + [" + this.lang + "]");
                    }
                    this.id = parts[2];
                }
            }
        }
    }

    private class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            GroovyScriptEngineService engine = (GroovyScriptEngineService) ScriptService.this.scriptEnginesByLang.get(GroovyScriptEngineService.NAME);
            if (engine != null) {
                String[] patches = settings.getAsArray(GroovyScriptEngineService.GROOVY_SCRIPT_BLACKLIST_PATCH, Strings.EMPTY_ARRAY);
                boolean blacklistChanged = engine.addToBlacklist(patches);
                if (blacklistChanged) {
                    logger.info("adding {} to [{}], new blacklisted methods: {}", patches, GroovyScriptEngineService.GROOVY_SCRIPT_BLACKLIST_PATCH, engine.blacklistAdditions());
                    engine.reloadConfig();
                    // Because the GroovyScriptEngineService knows nothing about the
                    // cache, we need to clear it here if the setting changes
                    ScriptService.this.clearCache();
                }
            }
        }
    }
}
