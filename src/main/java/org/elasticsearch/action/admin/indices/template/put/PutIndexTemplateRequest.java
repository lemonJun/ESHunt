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
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;

/**
 * A request to create an index template.
 */
public class PutIndexTemplateRequest extends MasterNodeOperationRequest<PutIndexTemplateRequest> implements IndicesRequest {

    private String name;

    private String cause = "";

    private String template;

    private int order;

    private boolean create;

    private Settings settings = EMPTY_SETTINGS;

    private Map<String, String> mappings = newHashMap();

    private final Set<Alias> aliases = newHashSet();

    private Map<String, IndexMetaData.Custom> customs = newHashMap();

    PutIndexTemplateRequest() {
    }

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (template == null) {
            validationException = addValidationError("template is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the index template.
     */
    public PutIndexTemplateRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    public PutIndexTemplateRequest template(String template) {
        this.template = template;
        return this;
    }

    public String template() {
        return this.template;
    }

    public PutIndexTemplateRequest order(int order) {
        this.order = order;
        return this;
    }

    public int order() {
        return this.order;
    }

    /**
     * Set to <tt>true</tt> to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link org.elasticsearch.indices.IndexTemplateAlreadyExistsException}.
     */
    public PutIndexTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index template with (either json/yaml/properties format).
     */
    public PutIndexTemplateRequest settings(String source) {
        this.settings = ImmutableSettings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * The settings to crete the index template with (either json/yaml/properties format).
     */
    public PutIndexTemplateRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    public Settings settings() {
        return this.settings;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(String type, String source) {
        mappings.put(type, source);
        return this;
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(String type, XContentBuilder source) {
        try {
            mappings.put(type, source.string());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to build json for mapping request", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(String type, Map<String, Object> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object> newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutIndexTemplateRequest mapping(String type, Object... source) {
        mapping(type, PutMappingRequest.buildFromSimplifiedDef(type, source));
        return this;
    }

    public Map<String, String> mappings() {
        return this.mappings;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(XContentBuilder templateBuilder) {
        try {
            return source(templateBuilder.bytes());
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("Failed to build json for template request", e);
        }
    }

    /**
     * The template source definition.
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest source(Map templateSource) {
        Map<String, Object> source = templateSource;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("template")) {
                template(entry.getValue().toString());
            } else if (name.equals("order")) {
                order(XContentMapValues.nodeIntegerValue(entry.getValue(), order()));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new ElasticsearchIllegalArgumentException("Malformed settings section, should include an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    if (!(entry1.getValue() instanceof Map)) {
                        throw new ElasticsearchIllegalArgumentException("Malformed mappings section for type [" + entry1.getKey() + "], should include an inner object describing the mapping");
                    }
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else if (name.equals("aliases")) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                // maybe custom?
                IndexMetaData.Custom.Factory factory = IndexMetaData.lookupFactory(name);
                if (factory != null) {
                    try {
                        customs.put(name, factory.fromMap((Map<String, Object>) entry.getValue()));
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse custom metadata for [" + name + "]");
                    }
                }
            }
        }
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(String templateSource) {
        try {
            return source(XContentFactory.xContent(templateSource).createParser(templateSource).mapOrderedAndClose());
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template source [" + templateSource + "]", e);
        }
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, int offset, int length) {
        try {
            return source(XContentFactory.xContent(source, offset, length).createParser(source, offset, length).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template source", e);
        }
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(BytesReference source) {
        try {
            return source(XContentFactory.xContent(source).createParser(source).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template source", e);
        }
    }

    public PutIndexTemplateRequest custom(IndexMetaData.Custom custom) {
        customs.put(custom.type(), custom);
        return this;
    }

    public Map<String, IndexMetaData.Custom> customs() {
        return this.customs;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest aliases(Map source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(XContentBuilder source) {
        return aliases(source.bytes());
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(BytesReference source) {
        try {
            XContentParser parser = XContentHelper.createParser(source);
            //move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse aliases", e);
        }
    }

    /**
     * Adds an alias that will be added when the index gets created.
     *
     * @param alias   The metadata for the new alias
     * @return  the index template creation request
     */
    public PutIndexTemplateRequest alias(Alias alias) {
        aliases.add(alias);
        return this;
    }

    @Override
    public String[] indices() {
        return new String[] { template };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpand();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cause = in.readString();
        name = in.readString();
        template = in.readString();
        order = in.readInt();
        create = in.readBoolean();
        settings = readSettingsFromStream(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readString(), in.readString());
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String type = in.readString();
            IndexMetaData.Custom customIndexMetaData = IndexMetaData.lookupFactorySafe(type).readFrom(in);
            customs.put(type, customIndexMetaData);
        }
        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            int aliasesSize = in.readVInt();
            for (int i = 0; i < aliasesSize; i++) {
                aliases.add(Alias.read(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(name);
        out.writeString(template);
        out.writeInt(order);
        out.writeBoolean(create);
        writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(customs.size());
        for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
            out.writeString(entry.getKey());
            IndexMetaData.lookupFactorySafe(entry.getKey()).writeTo(entry.getValue(), out);
        }
        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeVInt(aliases.size());
            for (Alias alias : aliases) {
                alias.writeTo(out);
            }
        }
    }
}
