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

package org.elasticsearch.common.io;

import com.google.common.collect.Iterators;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.XIOUtils;
import org.elasticsearch.common.logging.ESLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;

/**
 *
 */
public class FileSystemUtils {

    public static boolean mkdirs(File dir) {
        return dir.mkdirs();
    }

    public static boolean hasExtensions(File root, String... extensions) {
        if (root != null) {
            File[] children = root.listFiles();
            if (children != null) {
                for (File child : children) {
                    if (child.isDirectory()) {
                        boolean has = hasExtensions(child, extensions);
                        if (has) {
                            return true;
                        }
                    } else {
                        for (String extension : extensions) {
                            if (child.getName().endsWith(extension)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if at least one of the files exists.
     */
    public static boolean exists(File... files) {
        for (File file : files) {
            if (file.exists()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns <code>true</code> iff one of the files exists otherwise <code>false</code>
     */
    public static boolean exists(Path... files) {
        for (Path file : files) {
            if (Files.exists(file)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes the given files recursively. if <tt>deleteRoots</tt> is set to <code>true</code>
     * the given root files will be deleted as well. Otherwise only their content is deleted.
     */
    public static boolean deleteRecursively(File[] roots, boolean deleteRoots) {

        boolean deleted = true;
        for (File root : roots) {
            deleted &= deleteRecursively(root, deleteRoots);
        }
        return deleted;
    }

    /**
     * Deletes all subdirectories in the given path recursively
     * @throws java.lang.IllegalArgumentException if the given path is not a directory
     */
    public static void deleteSubDirectories(Path... paths) throws IOException {
        for (Path path : paths) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path subPath : stream) {
                    if (Files.isDirectory(subPath)) {
                        XIOUtils.rm(subPath);
                    }
                }
            }
        }
    }

    /**
     * Deletes the given files recursively including the given roots.
     */
    public static boolean deleteRecursively(File... roots) {
        return deleteRecursively(roots, true);
    }

    /**
     * Delete the supplied {@link java.io.File} - for directories,
     * recursively delete any nested directories or files as well.
     *
     * @param root       the root <code>File</code> to delete
     * @param deleteRoot whether or not to delete the root itself or just the content of the root.
     * @return <code>true</code> if the <code>File</code> was deleted,
     *         otherwise <code>false</code>
     */
    public static boolean deleteRecursively(File root, boolean deleteRoot) {
        if (root != null) {
            File[] children = root.listFiles();
            if (children != null) {
                for (File aChildren : children) {
                    deleteRecursively(aChildren, true);
                }
            }

            if (deleteRoot) {
                return root.delete();
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Check that a directory exists, is a directory and is readable
     * by the current user
     */
    public static boolean isAccessibleDirectory(File directory, ESLogger logger) {
        assert directory != null && logger != null;

        if (!directory.exists()) {
            logger.debug("[{}] directory does not exist.", directory.getAbsolutePath());
            return false;
        }
        if (!directory.isDirectory()) {
            logger.debug("[{}] should be a directory but is not.", directory.getAbsolutePath());
            return false;
        }
        if (!directory.canRead()) {
            logger.debug("[{}] directory is not readable.", directory.getAbsolutePath());
            return false;
        }
        return true;
    }

    private FileSystemUtils() {
    }

    /**
     * This utility copy a full directory content (excluded) under
     * a new directory but without overwriting existing files.
     *
     * When a file already exists in destination dir, the source file is copied under
     * destination directory but with a suffix appended if set or source file is ignored
     * if suffix is not set (null).
     * @param source Source directory (for example /tmp/es/src)
     * @param destination Destination directory (destination directory /tmp/es/dst)
     * @param suffix When not null, files are copied with a suffix appended to the original name (eg: ".new")
     *               When null, files are ignored
     */
    public static void moveFilesWithoutOverwriting(File source, final File destination, final String suffix) throws IOException {

        // Create destination dir
        FileSystemUtils.mkdirs(destination);

        final int configPathRootLevel = source.toPath().getNameCount();

        // We walk through the file tree from
        Files.walkFileTree(source.toPath(), new SimpleFileVisitor<Path>() {
            private Path buildPath(Path path) {
                return destination.toPath().resolve(path);
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // We are now in dir. We need to remove root of config files to have a relative path

                // If we are not walking in root dir, we might be able to copy its content
                // if it does not already exist
                if (configPathRootLevel != dir.getNameCount()) {
                    Path subpath = dir.subpath(configPathRootLevel, dir.getNameCount());
                    Path path = buildPath(subpath);
                    if (!Files.exists(path)) {
                        move(dir, path);
                        // We just ignore sub files from here
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path subpath = null;

                if (configPathRootLevel != file.getNameCount()) {
                    subpath = file.subpath(configPathRootLevel, file.getNameCount());
                }
                Path path = buildPath(subpath);

                if (!Files.exists(path)) {
                    // We just move the new file to new dir
                    Files.move(file, path);
                } else if (suffix != null) {
                    // If it already exists we try to copy this new version appending suffix to its name
                    path = Paths.get(path.toString().concat(suffix));
                    // We just move the file to new dir but with a new name (appended with suffix)
                    Files.move(file, path, StandardCopyOption.REPLACE_EXISTING);
                }

                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Copy recursively a dir to a new location
     * @param source source dir
     * @param destination destination dir
     */
    public static void copyDirectoryRecursively(File source, File destination) throws IOException {
        Files.walkFileTree(source.toPath(), new TreeCopier(source.toPath(), destination.toPath(), false));
    }

    /**
     * Move or rename a file to a target file. This method supports moving a file from
     * different filesystems (not supported by Files.move()).
     *
     * @param source      source file
     * @param destination destination file
     */
    public static void move(Path source, Path destination) throws IOException {
        try {
            // We can't use atomic move here since source & target can be on different filesystems.
            Files.move(source, destination);
        } catch (DirectoryNotEmptyException e) {
            Files.walkFileTree(source, new TreeCopier(source, destination, true));
        }
    }

    static class TreeCopier extends SimpleFileVisitor<Path> {
        private final Path source;
        private final Path target;
        private final boolean delete;

        TreeCopier(Path source, Path target, boolean delete) {
            this.source = source;
            this.target = target;
            this.delete = delete;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            Path newDir = target.resolve(source.relativize(dir));
            try {
                Files.copy(dir, newDir);
            } catch (FileAlreadyExistsException x) {
                // We ignore this
            } catch (IOException x) {
                return SKIP_SUBTREE;
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (delete) {
                deleteRecursively(dir.toFile(), true);
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Path newFile = target.resolve(source.relativize(file));
            try {
                Files.copy(file, newFile);
                if (delete) {
                    Files.deleteIfExists(file);
                }
            } catch (IOException x) {
                // We ignore this
            }
            return CONTINUE;
        }
    }

    /**
     * Returns an array of all files in the given directory.
     */
    public static Path[] files(Path directory) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

}
