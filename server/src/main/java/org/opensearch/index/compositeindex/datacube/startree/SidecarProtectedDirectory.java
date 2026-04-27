/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link FilterDirectory} wrapper that intercepts {@link #deleteFile(String)} calls to protect
 * sidecar star tree files from IndexWriter's garbage collection.
 * <p>
 * IndexWriter.deleteUnusedFiles() enumerates referenced files via SegmentInfos.files() and deletes
 * everything else. Since sidecar star tree files are not tracked in SegmentInfos, they would be
 * deleted without this protection layer. This wrapper silently skips deletion of any file in the
 * protected set, while delegating all other operations unchanged.
 * <p>
 * All other Directory methods delegate unchanged via {@link FilterDirectory}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SidecarProtectedDirectory extends FilterDirectory {

    private final Set<String> protectedFiles;

    /**
     * Creates a new SidecarProtectedDirectory wrapping the given directory.
     *
     * @param in the underlying directory to wrap
     */
    public SidecarProtectedDirectory(Directory in) {
        super(in);
        this.protectedFiles = ConcurrentHashMap.newKeySet();
    }

    /**
     * Creates a new SidecarProtectedDirectory wrapping the given directory with an initial set of
     * protected file names.
     *
     * @param in the underlying directory to wrap
     * @param initialProtectedFiles file names to protect from deletion
     */
    public SidecarProtectedDirectory(Directory in, Set<String> initialProtectedFiles) {
        super(in);
        this.protectedFiles = ConcurrentHashMap.newKeySet();
        this.protectedFiles.addAll(initialProtectedFiles);
    }

    /**
     * Intercepts file deletion. If the file is in the protected set, the deletion is silently
     * skipped (no-op). Otherwise, the deletion is delegated to the underlying directory.
     *
     * @param name the name of the file to delete
     * @throws IOException if the underlying directory throws during deletion
     */
    @Override
    public void deleteFile(String name) throws IOException {
        if (protectedFiles.contains(name)) {
            return;
        }
        super.deleteFile(name);
    }

    /**
     * Adds the given file names to the protected set. Protected files will not be deleted
     * by {@link #deleteFile(String)}.
     *
     * @param fileNames the file names to protect
     */
    public void protect(Set<String> fileNames) {
        protectedFiles.addAll(fileNames);
    }

    /**
     * Removes the given file names from the protected set. After removal, these files can
     * be deleted by {@link #deleteFile(String)}.
     *
     * @param fileNames the file names to unprotect
     */
    public void unprotect(Set<String> fileNames) {
        protectedFiles.removeAll(fileNames);
    }

    /**
     * Checks whether a file is currently in the protected set.
     *
     * @param fileName the file name to check
     * @return true if the file is protected from deletion
     */
    public boolean isProtected(String fileName) {
        return protectedFiles.contains(fileName);
    }

}
