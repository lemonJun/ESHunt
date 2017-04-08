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

package org.elasticsearch.script.groovy;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.codehaus.groovy.ast.ClassCodeExpressionTransformer;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.*;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "groovy";
    public static String GROOVY_SCRIPT_SANDBOX_ENABLED = "script.groovy.sandbox.enabled";
    public static String GROOVY_SCRIPT_BLACKLIST_PATCH = "script.groovy.sandbox.method_blacklist_patch";

    private final boolean sandboxed;
    private volatile GroovyClassLoader loader;
    private volatile Set<String> blacklistAdditions;

    @Inject
    public GroovyScriptEngineService(Settings settings) {
        super(settings);
        this.sandboxed = settings.getAsBoolean(GROOVY_SCRIPT_SANDBOX_ENABLED, false);
        this.blacklistAdditions = ImmutableSet.copyOf(settings.getAsArray(GROOVY_SCRIPT_BLACKLIST_PATCH, Strings.EMPTY_ARRAY));
        reloadConfig();
    }

    public Set<String> blacklistAdditions() {
        return this.blacklistAdditions;
    }

    /**
     * Appends the additional blacklisted methods to the current blacklist,
     * returns true if the black list has changed
     */
    public boolean addToBlacklist(String... additions) {
        Set<String> newBlackList = new HashSet<>(blacklistAdditions);
        Collections.addAll(newBlackList, additions);
        boolean changed = this.blacklistAdditions.equals(newBlackList) == false;
        this.blacklistAdditions = ImmutableSet.copyOf(newBlackList);
        return changed;
    }

    public void reloadConfig() {
        ImportCustomizer imports = new ImportCustomizer();
        imports.addStarImports("org.joda.time");
        imports.addStaticStars("java.lang.Math");
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(imports);
        if (this.sandboxed) {
            config.addCompilationCustomizers(GroovySandboxExpressionChecker.getSecureASTCustomizer(settings, this.blacklistAdditions));
        }
        // Add BigDecimal -> Double transformer
        config.addCompilationCustomizers(new GroovyBigDecimalTransformer(CompilePhase.CONVERSION));
        this.loader = new GroovyClassLoader(settings.getClassLoader(), config);
    }

    @Override
    public void close() {
        loader.clearCache();
        try {
            loader.close();
        } catch (IOException e) {
            logger.warn("Unable to close Groovy loader", e);
        }
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript script) {
        // script could be null, meaning the script has already been garbage collected
        if (script == null || NAME.equals(script.lang())) {
            // Clear the cache, this removes old script versions from the
            // cache to prevent running out of PermGen space
            loader.clearCache();
        }
    }

    @Override
    public String[] types() {
        return new String[] { NAME };
    }

    @Override
    public String[] extensions() {
        return new String[] { NAME };
    }

    @Override
    public boolean sandboxed() {
        return this.sandboxed;
    }

    @Override
    public Object compile(String script) {
        try {
            return loader.parseClass(script, Hashing.sha1().hashString(script, Charsets.UTF_8).toString());
        } catch (Throwable e) {
            if (logger.isTraceEnabled()) {
                logger.trace("exception compiling Groovy script:", e);
            }
            throw new GroovyScriptCompilationException(ExceptionsHelper.detailedMessage(e));
        }
    }

    /**
     * Return a script object with the given vars from the compiled script object
     */
    @SuppressWarnings("unchecked")
    private Script createScript(Object compiledScript, Map<String, Object> vars) throws InstantiationException, IllegalAccessException {
        Class scriptClass = (Class) compiledScript;
        Script scriptObject = (Script) scriptClass.newInstance();
        Binding binding = new Binding();
        binding.getVariables().putAll(vars);
        scriptObject.setBinding(binding);
        return scriptObject;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            return new GroovyScript(createScript(compiledScript, allVars), this.logger);
        } catch (Exception e) {
            throw new ScriptException("failed to build executable script", e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            allVars.putAll(lookup.asMap());
            if (vars != null) {
                allVars.putAll(vars);
            }
            Script scriptObject = createScript(compiledScript, allVars);
            return new GroovyScript(scriptObject, lookup, this.logger);
        } catch (Exception e) {
            throw new ScriptException("failed to build search script", e);
        }
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            Script scriptObject = createScript(compiledScript, allVars);
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException("failed to execute script", e);
        }
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    public static final class GroovyScript implements ExecutableScript, SearchScript {

        private final Script script;
        private final SearchLookup lookup;
        private final Map<String, Object> variables;
        private final ESLogger logger;

        public GroovyScript(Script script, ESLogger logger) {
            this(script, null, logger);
        }

        @SuppressWarnings("unchecked")
        public GroovyScript(Script script, @Nullable SearchLookup lookup, ESLogger logger) {
            this.script = script;
            this.lookup = lookup;
            this.logger = logger;
            this.variables = script.getBinding().getVariables();
        }

        @Override
        public void setScorer(Scorer scorer) {
            this.variables.put("_score", new ScoreAccessor(scorer));
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            if (lookup != null) {
                lookup.setNextReader(context);
            }
        }

        @Override
        public void setNextDocId(int doc) {
            if (lookup != null) {
                lookup.setNextDocId(doc);
            }
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public void setNextVar(String name, Object value) {
            variables.put(name, value);
        }

        @Override
        public void setNextSource(Map<String, Object> source) {
            if (lookup != null) {
                lookup.source().setNextSource(source);
            }
        }

        @Override
        public Object run() {
            try {
                return script.run();
            } catch (Throwable e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("exception running Groovy script", e);
                }
                throw new GroovyScriptExecutionException(ExceptionsHelper.detailedMessage(e));
            }
        }

        @Override
        public float runAsFloat() {
            return ((Number) run()).floatValue();
        }

        @Override
        public long runAsLong() {
            return ((Number) run()).longValue();
        }

        @Override
        public double runAsDouble() {
            return ((Number) run()).doubleValue();
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }

    }

    /**
     * A compilation customizer that is used to transform a number like 1.23,
     * which would normally be a BigDecimal, into a double value.
     */
    private class GroovyBigDecimalTransformer extends CompilationCustomizer {

        private GroovyBigDecimalTransformer(CompilePhase phase) {
            super(phase);
        }

        @Override
        public void call(final SourceUnit source, final GeneratorContext context, final ClassNode classNode) throws CompilationFailedException {
            new BigDecimalExpressionTransformer(source).visitClass(classNode);
        }
    }

    /**
     * Groovy expression transformer that converts BigDecimals to doubles
     */
    private class BigDecimalExpressionTransformer extends ClassCodeExpressionTransformer {

        private final SourceUnit source;

        private BigDecimalExpressionTransformer(SourceUnit source) {
            this.source = source;
        }

        @Override
        protected SourceUnit getSourceUnit() {
            return this.source;
        }

        @Override
        public Expression transform(Expression expr) {
            Expression newExpr = expr;
            if (expr instanceof ConstantExpression) {
                ConstantExpression constExpr = (ConstantExpression) expr;
                Object val = constExpr.getValue();
                if (val != null && val instanceof BigDecimal) {
                    newExpr = new ConstantExpression(((BigDecimal) val).doubleValue());
                }
            }
            return super.transform(newExpr);
        }
    }

}
