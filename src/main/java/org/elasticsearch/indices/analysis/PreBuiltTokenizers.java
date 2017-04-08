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
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.XPatternTokenizer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.io.Reader;
import java.util.Locale;

/**
 *
 */
public enum PreBuiltTokenizers {

    STANDARD(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new StandardTokenizer(version.luceneVersion, reader);
        }
    },

    CLASSIC(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new ClassicTokenizer(version.luceneVersion, reader);
        }
    },

    UAX_URL_EMAIL(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new UAX29URLEmailTokenizer(version.luceneVersion, reader);
        }
    },

    PATH_HIERARCHY(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new PathHierarchyTokenizer(reader);
        }
    },

    KEYWORD(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new KeywordTokenizer(reader);
        }
    },

    LETTER(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new LetterTokenizer(version.luceneVersion, reader);
        }
    },

    LOWERCASE(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new LowerCaseTokenizer(version.luceneVersion, reader);
        }
    },

    WHITESPACE(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new WhitespaceTokenizer(version.luceneVersion, reader);
        }
    },

    NGRAM(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new NGramTokenizer(version.luceneVersion, reader);
        }
    },

    EDGE_NGRAM(CachingStrategy.LUCENE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new EdgeNGramTokenizer(version.luceneVersion, reader, EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
        }
    },

    PATTERN(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new XPatternTokenizer(reader, Regex.compile("\\W+", null), -1);
        }
    },

    THAI(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Reader reader, Version version) {
            return new ThaiTokenizer(reader);
        }
    }

    ;

    abstract protected Tokenizer create(Reader reader, Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<TokenizerFactory> cache;

    PreBuiltTokenizers(CachingStrategy cachingStrategy) {
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public synchronized TokenizerFactory getTokenizerFactory(final Version version) {
        TokenizerFactory tokenizerFactory = cache.get(version);
        if (tokenizerFactory == null) {
            final String finalName = name();

            tokenizerFactory = new TokenizerFactory() {
                @Override
                public String name() {
                    return finalName.toLowerCase(Locale.ROOT);
                }

                @Override
                public Tokenizer create(Reader reader) {
                    return valueOf(finalName).create(reader, version);
                }
            };
            cache.put(version, tokenizerFactory);
        }

        return tokenizerFactory;
    }

    /**
     * Get a pre built Tokenizer by its name or fallback to the default one
     * @param name Tokenizer name
     * @param defaultTokenizer default Tokenizer if name not found
     */
    public static PreBuiltTokenizers getOrDefault(String name, PreBuiltTokenizers defaultTokenizer) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return defaultTokenizer;
        }
    }
}
