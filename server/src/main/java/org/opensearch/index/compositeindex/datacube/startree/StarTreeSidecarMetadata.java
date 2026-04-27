/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages a generational per-shard metadata file that tracks which segments have sidecar star tree files.
 * <p>
 * The metadata file follows a generational pattern: {@code _startree_sidecar_gen0.meta},
 * {@code _startree_sidecar_gen1.meta}, etc. Only the current generation file exists on disk at
 * steady state. On shard start, the recovery scan deletes all generation files except the highest
 * valid one (handles crash between write of genN+1 and delete of genN).
 * <p>
 * The in-memory state is a {@link ConcurrentHashMap} mapping segment names to {@link SidecarSegmentEntry}
 * records containing the set of sidecar file names for that segment.
 * <p>
 * JSON format (version=1):
 * <pre>
 * {
 *   "version": 1,
 *   "generation": N,
 *   "segments": {
 *     "_0": { "files": ["_0.cid", "_0.cim"] },
 *     "_1": { "files": ["_1.cid", "_1.cim"] }
 *   }
 * }
 * </pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeSidecarMetadata {

    private static final Logger logger = LogManager.getLogger(StarTreeSidecarMetadata.class);

    /** Prefix for sidecar metadata generation files. */
    static final String FILE_PREFIX = "_startree_sidecar_gen";

    /** Suffix for sidecar metadata generation files. */
    static final String FILE_SUFFIX = ".meta";

    /** Pattern to match generation files and extract the generation number. */
    static final Pattern GEN_FILE_PATTERN = Pattern.compile("^_startree_sidecar_gen(\\d+)\\.meta$");

    /** Current JSON format version for forward compatibility. */
    static final int FORMAT_VERSION = 1;

    /** JSON field names. */
    private static final String FIELD_VERSION = "version";
    private static final String FIELD_GENERATION = "generation";
    private static final String FIELD_SEGMENTS = "segments";
    private static final String FIELD_FILES = "files";

    /**
     * A simple record containing the set of sidecar file names for a segment.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class SidecarSegmentEntry {
        private final Set<String> files;

        /**
         * Creates a new entry with the given sidecar file names.
         *
         * @param files the sidecar file names for this segment
         */
        public SidecarSegmentEntry(Set<String> files) {
            this.files = new HashSet<>(files);
        }

        /**
         * Returns the sidecar file names for this segment.
         *
         * @return unmodifiable view of the file names
         */
        public Set<String> getFiles() {
            return Collections.unmodifiableSet(files);
        }
    }

    private final ConcurrentHashMap<String, SidecarSegmentEntry> segments;
    private long generation;

    /**
     * Creates an empty metadata instance with generation=-1 (no gen file on disk).
     */
    public StarTreeSidecarMetadata() {
        this.segments = new ConcurrentHashMap<>();
        this.generation = -1;
    }

    /**
     * Creates a metadata instance with the given segments and generation.
     *
     * @param segments the segment entries loaded from disk
     * @param generation the generation number loaded from disk
     */
    private StarTreeSidecarMetadata(Map<String, SidecarSegmentEntry> segments, long generation) {
        this.segments = new ConcurrentHashMap<>(segments);
        this.generation = generation;
    }

    /**
     * Returns the file name for a given generation number.
     *
     * @param gen the generation number
     * @return the file name, e.g. {@code _startree_sidecar_gen0.meta}
     */
    static String genFileName(long gen) {
        return FILE_PREFIX + gen + FILE_SUFFIX;
    }

    /**
     * Static factory method. Scans for {@code _startree_sidecar_gen*.meta} files, loads the highest
     * valid generation. If the highest gen file is corrupt (invalid JSON), falls back to the previous
     * generation. On startup, deletes all generation files except the highest valid one.
     * <p>
     * If no gen files are found, returns an empty instance with generation=-1.
     *
     * @param directory the Lucene directory to scan
     * @return a new {@code StarTreeSidecarMetadata} instance with the loaded state
     * @throws IOException if an I/O error occurs during scanning or loading
     */
    public static StarTreeSidecarMetadata load(Directory directory) throws IOException {
        String[] allFiles = directory.listAll();

        // Collect all generation numbers from matching files
        List<Long> generations = new ArrayList<>();
        for (String file : allFiles) {
            Matcher matcher = GEN_FILE_PATTERN.matcher(file);
            if (matcher.matches()) {
                try {
                    generations.add(Long.parseLong(matcher.group(1)));
                } catch (NumberFormatException e) {
                    logger.warn("Ignoring sidecar metadata file with unparseable generation: {}", file);
                }
            }
        }

        if (generations.isEmpty()) {
            return new StarTreeSidecarMetadata();
        }

        // Sort descending so we try highest generation first
        generations.sort(Collections.reverseOrder());

        StarTreeSidecarMetadata loaded = null;
        long loadedGen = -1;

        // Try to load from highest generation, fall back to previous if corrupt
        for (Long gen : generations) {
            String fileName = genFileName(gen);
            try {
                loaded = loadFromFile(directory, fileName, gen);
                loadedGen = gen;
                break;
            } catch (Exception e) {
                logger.warn("Failed to load sidecar metadata from {} (corrupt or invalid), trying previous generation", fileName, e);
            }
        }

        // Delete all generation files except the one we loaded
        for (Long gen : generations) {
            if (gen != loadedGen) {
                String fileName = genFileName(gen);
                try {
                    directory.deleteFile(fileName);
                    logger.debug("Deleted stale sidecar metadata generation file: {}", fileName);
                } catch (IOException e) {
                    logger.warn("Failed to delete stale sidecar metadata file: {}", fileName, e);
                }
            }
        }

        if (loaded == null) {
            logger.warn("All sidecar metadata generation files were corrupt, starting with empty state");
            return new StarTreeSidecarMetadata();
        }

        return loaded;
    }

    /**
     * Loads metadata from a specific generation file.
     */
    private static StarTreeSidecarMetadata loadFromFile(Directory directory, String fileName, long expectedGen) throws IOException {
        byte[] bytes;
        try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
            int length = (int) input.length();
            bytes = new byte[length];
            input.readBytes(bytes, 0, length);
        }

        String json = new String(bytes, StandardCharsets.UTF_8);
        Map<String, SidecarSegmentEntry> segmentEntries = new ConcurrentHashMap<>();
        long parsedGeneration = -1;

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            // Expect START_OBJECT
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IOException("Expected START_OBJECT, got " + token);
            }

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (FIELD_VERSION.equals(currentFieldName)) {
                    int version = parser.intValue();
                    if (version != FORMAT_VERSION) {
                        throw new IOException("Unsupported sidecar metadata version: " + version);
                    }
                } else if (FIELD_GENERATION.equals(currentFieldName)) {
                    parsedGeneration = parser.longValue();
                } else if (FIELD_SEGMENTS.equals(currentFieldName) && token == XContentParser.Token.START_OBJECT) {
                    parseSegments(parser, segmentEntries);
                }
            }
        }

        if (parsedGeneration != expectedGen) {
            logger.warn("Sidecar metadata generation mismatch: file={}, parsed={}, expected={}", fileName, parsedGeneration, expectedGen);
        }

        return new StarTreeSidecarMetadata(segmentEntries, parsedGeneration);
    }

    /**
     * Parses the "segments" object from the JSON.
     */
    private static void parseSegments(XContentParser parser, Map<String, SidecarSegmentEntry> segmentEntries) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String segmentName = parser.currentName();
                token = parser.nextToken(); // START_OBJECT for this segment
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IOException("Expected START_OBJECT for segment " + segmentName + ", got " + token);
                }
                Set<String> files = parseSegmentEntry(parser);
                segmentEntries.put(segmentName, new SidecarSegmentEntry(files));
            }
        }
    }

    /**
     * Parses a single segment entry object: { "files": ["file1", "file2"] }
     */
    private static Set<String> parseSegmentEntry(XContentParser parser) throws IOException {
        Set<String> files = new HashSet<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && FIELD_FILES.equals(parser.currentName())) {
                token = parser.nextToken(); // START_ARRAY
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new IOException("Expected START_ARRAY for files, got " + token);
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        files.add(parser.text());
                    }
                }
            }
        }
        return files;
    }

    /**
     * Checks if the given segment has sidecar star tree data.
     *
     * @param segmentName the segment name to check
     * @return true if the segment has a sidecar entry
     */
    public boolean hasStarTreeData(String segmentName) {
        return segments.containsKey(segmentName);
    }

    /**
     * Registers sidecar files for a segment in the in-memory map.
     *
     * @param segmentName the segment name
     * @param files the sidecar file names for this segment
     */
    public void register(String segmentName, Set<String> files) {
        segments.put(segmentName, new SidecarSegmentEntry(files));
    }

    /**
     * Removes a segment entry from the in-memory map.
     *
     * @param segmentName the segment name to remove
     * @return the removed entry's file names, or an empty set if no entry existed
     */
    public Set<String> remove(String segmentName) {
        SidecarSegmentEntry removed = segments.remove(segmentName);
        if (removed == null) {
            return Collections.emptySet();
        }
        return removed.getFiles();
    }

    /**
     * Returns all segment names that have sidecar star tree data.
     *
     * @return unmodifiable set of segment names
     */
    public Set<String> getSegmentNames() {
        return Collections.unmodifiableSet(segments.keySet());
    }

    /**
     * Returns the sidecar file names for a given segment.
     *
     * @param segmentName the segment name
     * @return the file names, or an empty set if no entry exists
     */
    public Set<String> getStarTreeFiles(String segmentName) {
        SidecarSegmentEntry entry = segments.get(segmentName);
        if (entry == null) {
            return Collections.emptySet();
        }
        return entry.getFiles();
    }

    /**
     * Returns the union of all sidecar file names across all segments, plus the current metadata
     * generation file itself. This is used for the protected set and commit data.
     *
     * @return set of all sidecar file names including the metadata gen file
     */
    public Set<String> getAllSidecarFileNames() {
        Set<String> allFiles = new HashSet<>();
        for (SidecarSegmentEntry entry : segments.values()) {
            allFiles.addAll(entry.getFiles());
        }
        // Include the current metadata gen file itself
        if (generation >= 0) {
            allFiles.add(genFileName(generation));
        }
        return allFiles;
    }

    /**
     * Checks if any segment entry contains the given file name.
     *
     * @param fileName the file name to check
     * @return true if any segment entry contains this file
     */
    public boolean containsFile(String fileName) {
        // Also check if it's the current metadata gen file
        if (generation >= 0 && genFileName(generation).equals(fileName)) {
            return true;
        }
        for (SidecarSegmentEntry entry : segments.values()) {
            if (entry.getFiles().contains(fileName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if no segments have sidecar data.
     *
     * @return true if the metadata is empty
     */
    public boolean isEmpty() {
        return segments.isEmpty();
    }

    /**
     * Returns the current generation number.
     *
     * @return the generation number, or -1 if no gen file has been written
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Writes {@code _startree_sidecar_genN+1.meta} atomically:
     * <ol>
     *   <li>Serialize to JSON</li>
     *   <li>Write to the new gen file via {@code directory.createOutput()}</li>
     *   <li>Fsync via {@code directory.sync()}</li>
     *   <li>Delete the previous generation file</li>
     *   <li>Increment the internal generation counter</li>
     * </ol>
     *
     * @param directory the Lucene directory to write to
     * @throws IOException if an I/O error occurs during writing
     */
    public void commit(Directory directory) throws IOException {
        long newGen = generation + 1;
        String newFileName = genFileName(newGen);
        String previousFileName = generation >= 0 ? genFileName(generation) : null;

        // Serialize to JSON
        byte[] jsonBytes = serializeToJson(newGen);

        // Write to the new gen file
        try (IndexOutput output = directory.createOutput(newFileName, IOContext.DEFAULT)) {
            output.writeBytes(jsonBytes, jsonBytes.length);
        }

        // Fsync the new file
        directory.sync(Collections.singleton(newFileName));

        // Delete the previous generation file
        if (previousFileName != null) {
            try {
                directory.deleteFile(previousFileName);
            } catch (IOException e) {
                logger.warn("Failed to delete previous sidecar metadata generation file: {}", previousFileName, e);
            }
        }

        // Update internal generation
        generation = newGen;
        logger.debug("Committed sidecar metadata generation {} with {} segments", newGen, segments.size());
    }

    /**
     * Serializes the current state to JSON bytes.
     */
    private byte[] serializeToJson(long gen) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (XContentBuilder builder = XContentFactory.jsonBuilder(baos)) {
            builder.startObject();
            builder.field(FIELD_VERSION, FORMAT_VERSION);
            builder.field(FIELD_GENERATION, gen);
            builder.startObject(FIELD_SEGMENTS);
            for (Map.Entry<String, SidecarSegmentEntry> entry : segments.entrySet()) {
                builder.startObject(entry.getKey());
                builder.startArray(FIELD_FILES);
                for (String file : entry.getValue().getFiles()) {
                    builder.value(file);
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }
        return baos.toByteArray();
    }

    /**
     * Removes entries for segments not present in the given {@link SegmentInfos}. Returns the set
     * of orphaned sidecar file names for cleanup.
     *
     * @param currentInfos the current segment infos to compare against
     * @return the set of orphaned sidecar file names that were removed
     */
    public Set<String> removeOrphanedSegments(SegmentInfos currentInfos) {
        Set<String> currentSegmentNames = new HashSet<>();
        for (SegmentCommitInfo commitInfo : currentInfos) {
            currentSegmentNames.add(commitInfo.info.name);
        }

        Set<String> orphanedFiles = new HashSet<>();
        List<String> orphanedSegments = new ArrayList<>();

        for (Map.Entry<String, SidecarSegmentEntry> entry : segments.entrySet()) {
            if (currentSegmentNames.contains(entry.getKey()) == false) {
                orphanedSegments.add(entry.getKey());
                orphanedFiles.addAll(entry.getValue().getFiles());
            }
        }

        for (String segmentName : orphanedSegments) {
            segments.remove(segmentName);
        }

        if (orphanedSegments.isEmpty() == false) {
            logger.debug("Removed {} orphaned sidecar segments: {}", orphanedSegments.size(), orphanedSegments);
        }

        return orphanedFiles;
    }
}
