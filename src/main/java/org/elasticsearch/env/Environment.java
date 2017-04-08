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

package org.elasticsearch.env;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * The environment of where things exists.
 */
public class Environment {

    private final Settings settings;

    private final File homeFile;

    private final File workFile;

    private final File workWithClusterFile;

    private final File[] dataFiles;

    private final File[] dataWithClusterFiles;

    private final File[] repoFiles;

    private final File configFile;

    private final File pluginsFile;

    private final File logsFile;

    public Environment() {
        this(EMPTY_SETTINGS);
    }

    public Environment(Settings settings) {
        this.settings = settings;
        if (settings.get("path.home") != null) {
            homeFile = new File(cleanPath(settings.get("path.home")));
        } else {
            homeFile = new File(System.getProperty("user.dir"));
        }

        if (settings.get("path.conf") != null) {
            configFile = new File(cleanPath(settings.get("path.conf")));
        } else {
            configFile = new File(homeFile, "config");
        }

        if (settings.get("path.plugins") != null) {
            pluginsFile = new File(cleanPath(settings.get("path.plugins")));
        } else {
            pluginsFile = new File(homeFile, "plugins");
        }

        if (settings.get("path.work") != null) {
            workFile = new File(cleanPath(settings.get("path.work")));
        } else {
            workFile = new File(homeFile, "work");
        }
        workWithClusterFile = new File(workFile, ClusterName.clusterNameFromSettings(settings).value());

        String[] dataPaths = settings.getAsArray("path.data");
        if (dataPaths.length > 0) {
            dataFiles = new File[dataPaths.length];
            dataWithClusterFiles = new File[dataPaths.length];
            for (int i = 0; i < dataPaths.length; i++) {
                dataFiles[i] = new File(dataPaths[i]);
                dataWithClusterFiles[i] = new File(dataFiles[i], ClusterName.clusterNameFromSettings(settings).value());
            }
        } else {
            dataFiles = new File[] { new File(homeFile, "data") };
            dataWithClusterFiles = new File[] { new File(new File(homeFile, "data"), ClusterName.clusterNameFromSettings(settings).value()) };
        }

        String[] repoPaths = settings.getAsArray("path.repo");
        if (repoPaths.length > 0) {
            repoFiles = new File[repoPaths.length];
            for (int i = 0; i < repoPaths.length; i++) {
                repoFiles[i] = new File(repoPaths[i]);
            }
        } else {
            repoFiles = new File[0];
        }

        if (settings.get("path.logs") != null) {
            logsFile = new File(cleanPath(settings.get("path.logs")));
        } else {
            logsFile = new File(homeFile, "logs");
        }
    }

    /**
     * The settings used to build this environment.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * The home of the installation.
     */
    public File homeFile() {
        return homeFile;
    }

    /**
     * The work location, path to temp files.
     *
     * Note, currently, we don't use it in ES at all, we should strive to see if we can keep it like that,
     * but if we do, we have the infra for it.
     */
    public File workFile() {
        return workFile;
    }

    /**
     * The work location with the cluster name as a sub directory.
     *
     * Note, currently, we don't use it in ES at all, we should strive to see if we can keep it like that,
     * but if we do, we have the infra for it.
     */
    public File workWithClusterFile() {
        return workWithClusterFile;
    }

    /**
     * The data location.
     */
    public File[] dataFiles() {
        return dataFiles;
    }

    /**
     * The data location with the cluster name as a sub directory.
     */
    public File[] dataWithClusterFiles() {
        return dataWithClusterFiles;
    }

    /**
     * The shared filesystem repo locations.
     */
    public File[] repoFiles() {
        return repoFiles;
    }

    /**
     * Resolves the specified location against the list of configured repository roots
     *
     * If the specified location doesn't match any of the roots, returns null.
     */
    public File resolveRepoFile(String location) {
        return resolve(repoFiles, location);
    }

    /**
     * Checks if the specified URL is pointing to the local file system and if it does, resolves the specified url
     * against the list of configured repository roots
     *
     * If the specified url doesn't match any of the roots, returns null.
     */
    public URL resolveRepoURL(URL url) {
        try {
            if ("file".equalsIgnoreCase(url.getProtocol())) {
                if (url.getHost() == null || "".equals(url.getHost())) {
                    // only local file urls are supported
                    File file = resolve(repoFiles, url.toURI());
                    if (file == null) {
                        // Couldn't resolve against known repo locations
                        return null;
                    }
                    // Normalize URL
                    return file.toURI().toURL();
                }
                return null;
            } else if ("jar".equals(url.getProtocol())) {
                String file = url.toURI().getSchemeSpecificPart();
                int pos = file.indexOf("!/");
                if (pos < 0) {
                    return null;
                }
                String jarTail = file.substring(pos);
                String filePath = file.substring(0, pos);
                URL internalUrl = new URL(filePath);
                URL normalizedUrl = resolveRepoURL(internalUrl);
                if (normalizedUrl == null) {
                    return null;
                }
                return new URL("jar", "", normalizedUrl.toExternalForm() + jarTail);
            } else {
                // It's not file or jar url and it didn't match the white list - reject
                return null;
            }
        } catch (MalformedURLException ex) {
            // cannot make sense of this file url
            return null;
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    /**
     * The config location.
     */
    public File configFile() {
        return configFile;
    }

    public File pluginsFile() {
        return pluginsFile;
    }

    public File logsFile() {
        return logsFile;
    }

    public String resolveConfigAndLoadToString(String path) throws FailedToResolveConfigException, IOException {
        return Streams.copyToString(new InputStreamReader(resolveConfig(path).openStream(), Charsets.UTF_8));
    }

    public URL resolveConfig(String path) throws FailedToResolveConfigException {
        String origPath = path;
        // first, try it as a path on the file system
        File f1 = new File(path);
        if (f1.exists()) {
            try {
                return f1.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new FailedToResolveConfigException("Failed to resolve path [" + f1 + "]", e);
            }
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        // next, try it relative to the config location
        File f2 = new File(configFile, path);
        if (f2.exists()) {
            try {
                return f2.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new FailedToResolveConfigException("Failed to resolve path [" + f2 + "]", e);
            }
        }
        // try and load it from the classpath directly
        URL resource = settings.getClassLoader().getResource(path);
        if (resource != null) {
            return resource;
        }
        // try and load it from the classpath with config/ prefix
        if (!path.startsWith("config/")) {
            resource = settings.getClassLoader().getResource("config/" + path);
            if (resource != null) {
                return resource;
            }
        }
        throw new FailedToResolveConfigException("Failed to resolve config path [" + origPath + "], tried file path [" + f1 + "], path file [" + f2 + "], and classpath");
    }

    /**
     * Tries to resolve the given path against the list of available roots.
     *
     * If path starts with one of the listed roots, it returned back by this method, otherwise null is returned.
     */
    public static File resolve(File[] roots, String path) {
        for (File root : roots) {
            Path rootPath = root.toPath().normalize();
            Path normalizedPath = rootPath.resolve(path).normalize();
            if (normalizedPath.startsWith(rootPath)) {
                return normalizedPath.toFile();
            }
        }
        return null;
    }

    /**
     * Tries to resolve the given path against the list of available roots.
     *
     * If path starts with one of the listed roots, it returned back by this method, otherwise null is returned.
     */
    public static File resolve(File[] roots, URI uri) {
        return resolve(roots, Paths.get(uri).normalize().toString());
    }

}
