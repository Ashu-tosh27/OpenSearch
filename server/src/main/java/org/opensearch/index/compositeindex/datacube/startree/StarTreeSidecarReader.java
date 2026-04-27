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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.LuceneDocValuesProducerFactory;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.DimensionConfig;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.getFieldInfoList;

/**
 * Opens sidecar star tree files directly from the directory (independent of the segment's codec)
 * and provides star tree values via the {@link StarTreeValuesProvider} interface.
 * <p>
 * Uses Lucene-style CAS reference counting for safe concurrent access. The cache holds the initial
 * reference (refCount starts at 1). Query threads call {@link #incRef()} before use and
 * {@link #decRef()} in a finally block. When refCount reaches 0, file handles are closed and
 * (if {@link #markPendingDeletion()} was called) sidecar files are unprotected and deleted.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeSidecarReader implements StarTreeValuesProvider, Closeable {
    private static final Logger logger = LogManager.getLogger(StarTreeSidecarReader.class);

    /** Sidecar files use empty string as segment suffix */
    private static final String SIDECAR_SEGMENT_SUFFIX = "";

    private final SidecarProtectedDirectory sidecarProtectedDirectory;
    private final Set<String> starTreeFiles;

    private IndexInput dataIn;
    private final Map<String, IndexInput> compositeIndexInputMap = new LinkedHashMap<>();
    private final Map<String, CompositeIndexMetadata> compositeIndexMetadataMap = new LinkedHashMap<>();
    private final List<CompositeIndexFieldInfo> compositeFieldInfos = new ArrayList<>();
    private DocValuesProducer compositeDocValuesProducer;
    private SegmentReadState readState;

    // Lucene-style CAS reference counting
    private final AtomicInteger refCount = new AtomicInteger(1); // starts at 1 (cache holds initial ref)
    private volatile boolean pendingDeletion = false;

    /**
     * Opens sidecar star tree files for the given segment.
     *
     * @param directory                  the index directory to open files from
     * @param segmentName                the segment name (e.g., "_0")
     * @param starTreeFiles              the sidecar file names from metadata
     * @param sidecarProtectedDirectory  for deferred file deletion
     * @throws IOException if an I/O error occurs or files are missing (stale entry)
     */
    public StarTreeSidecarReader(
        Directory directory,
        String segmentName,
        Set<String> starTreeFiles,
        SidecarProtectedDirectory sidecarProtectedDirectory,
        byte[] segmentId,
        int maxDoc
    ) throws IOException {
        this.sidecarProtectedDirectory = sidecarProtectedDirectory;
        this.starTreeFiles = starTreeFiles;

        String metaFileName = IndexFileNames.segmentFileName(
            segmentName,
            SIDECAR_SEGMENT_SUFFIX,
            Composite912DocValuesFormat.META_EXTENSION
        );
        String dataFileName = IndexFileNames.segmentFileName(
            segmentName,
            SIDECAR_SEGMENT_SUFFIX,
            Composite912DocValuesFormat.DATA_EXTENSION
        );

        List<String> fields = new ArrayList<>();
        boolean success = false;
        try {
            // Construct a minimal SegmentInfo for header validation and SegmentReadState
            SegmentInfo segmentInfo = new SegmentInfo(
                directory,
                Version.LATEST,
                Version.LATEST,
                segmentName,
                maxDoc,
                false, // useCompoundFile
                false, // hasBlocks
                null, // codec — not needed for sidecar reading
                Collections.emptyMap(), // diagnostics
                segmentId,
                Collections.emptyMap(), // attributes
                null // indexSort
            );

            try (ChecksumIndexInput metaIn = directory.openChecksumInput(metaFileName)) {
                // Open and validate data file
                dataIn = directory.openInput(dataFileName, IOContext.DEFAULT);
                CodecUtil.checkIndexHeader(
                    dataIn,
                    Composite912DocValuesFormat.DATA_CODEC_NAME,
                    Composite912DocValuesFormat.VERSION_START,
                    Composite912DocValuesFormat.VERSION_CURRENT,
                    segmentId,
                    SIDECAR_SEGMENT_SUFFIX
                );

                // Parse meta file
                Throwable priorE = null;
                try {
                    CodecUtil.checkIndexHeader(
                        metaIn,
                        Composite912DocValuesFormat.META_CODEC_NAME,
                        Composite912DocValuesFormat.VERSION_START,
                        Composite912DocValuesFormat.VERSION_CURRENT,
                        segmentId,
                        SIDECAR_SEGMENT_SUFFIX
                    );

                    Map<String, DocValuesType> dimensionFieldTypeMap = new HashMap<>();
                    while (true) {
                        long magicMarker = metaIn.readLong();
                        if (magicMarker == -1) {
                            break;
                        } else if (magicMarker < 0) {
                            throw new CorruptIndexException("Unknown token encountered: " + magicMarker, metaIn);
                        } else if (COMPOSITE_FIELD_MARKER != magicMarker) {
                            logger.error("Invalid composite field magic marker");
                            throw new IOException("Invalid composite field magic marker");
                        }

                        int version = metaIn.readVInt();
                        if (VERSION_CURRENT != version) {
                            logger.error("Invalid composite field version");
                            throw new IOException("Invalid composite field version");
                        }

                        String compositeFieldName = metaIn.readString();
                        CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType
                            .fromName(metaIn.readString());

                        switch (compositeFieldType) {
                            case STAR_TREE:
                                StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
                                    metaIn,
                                    compositeFieldName,
                                    compositeFieldType,
                                    version
                                );
                                compositeFieldInfos.add(new CompositeIndexFieldInfo(compositeFieldName, compositeFieldType));

                                IndexInput starTreeIndexInput = dataIn.slice(
                                    "star-tree data slice for respective star-tree fields",
                                    starTreeMetadata.getDataStartFilePointer(),
                                    starTreeMetadata.getDataLength()
                                );
                                compositeIndexInputMap.put(compositeFieldName, starTreeIndexInput);
                                compositeIndexMetadataMap.put(compositeFieldName, starTreeMetadata);

                                Map<String, DimensionConfig> dimensionFieldToDocValuesMap = starTreeMetadata.getDimensionFields();
                                for (Map.Entry<String, DimensionConfig> dimensionEntry : dimensionFieldToDocValuesMap.entrySet()) {
                                    String dimName = fullyQualifiedFieldNameForStarTreeDimensionsDocValues(
                                        compositeFieldName,
                                        dimensionEntry.getKey()
                                    );
                                    fields.add(dimName);
                                    dimensionFieldTypeMap.put(dimName, dimensionEntry.getValue().getDocValuesType());
                                }
                                for (Metric metric : starTreeMetadata.getMetrics()) {
                                    for (MetricStat metricStat : metric.getBaseMetrics()) {
                                        fields.add(
                                            fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                                                compositeFieldName,
                                                metric.getField(),
                                                metricStat.getTypeName()
                                            )
                                        );
                                    }
                                }
                                break;
                            default:
                                throw new CorruptIndexException("Invalid composite field type found in the file", dataIn);
                        }
                    }

                    // Build FieldInfos and SegmentReadState for the doc values producer
                    FieldInfos fieldInfos = new FieldInfos(getFieldInfoList(fields, dimensionFieldTypeMap));
                    this.readState = new SegmentReadState(directory, segmentInfo, fieldInfos, IOContext.DEFAULT, SIDECAR_SEGMENT_SUFFIX);

                    // Initialize star-tree doc values producer
                    compositeDocValuesProducer = LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
                        Composite912Codec.COMPOSITE_INDEX_CODEC_NAME,
                        this.readState,
                        Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
                        Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                        Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
                        Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
                    );

                } catch (Throwable t) {
                    priorE = t;
                } finally {
                    CodecUtil.checkFooter(metaIn, priorE);
                }
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public List<CompositeIndexFieldInfo> getCompositeIndexFields() {
        return compositeFieldInfos;
    }

    @Override
    public CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo compositeIndexFieldInfo) throws IOException {
        switch (compositeIndexFieldInfo.getType()) {
            case STAR_TREE:
                return new StarTreeValues(
                    compositeIndexMetadataMap.get(compositeIndexFieldInfo.getField()),
                    compositeIndexInputMap.get(compositeIndexFieldInfo.getField()),
                    compositeDocValuesProducer,
                    this.readState
                );
            default:
                throw new CorruptIndexException("Unsupported composite index field type: ", compositeIndexFieldInfo.getType().getName());
        }
    }

    /**
     * Increments the reference count. Callers must call {@link #decRef()} in a finally block.
     *
     * @throws AlreadyClosedException if the reader has already been closed (refCount was 0)
     */
    public void incRef() {
        // CAS loop — same pattern as Lucene's AbstractRefCounted
        while (true) {
            int count = refCount.get();
            if (count <= 0) {
                throw new AlreadyClosedException("StarTreeSidecarReader already closed");
            }
            if (refCount.compareAndSet(count, count + 1)) {
                return;
            }
        }
    }

    /**
     * Decrements the reference count. When the count reaches 0, file handles are closed.
     * If {@link #markPendingDeletion()} was called, sidecar files are also deleted.
     *
     * @throws IOException if an I/O error occurs during close or deletion
     */
    public void decRef() throws IOException {
        int count = refCount.decrementAndGet();
        assert count >= 0 : "refCount underflow";
        if (count == 0) {
            closeInternal();
            if (pendingDeletion) {
                deleteFiles();
            }
        }
    }

    /**
     * Marks this reader for deferred file deletion. When refCount reaches 0, the sidecar files
     * will be unprotected and deleted from disk.
     */
    public void markPendingDeletion() {
        pendingDeletion = true;
    }

    @Override
    public void close() throws IOException {
        decRef();
    }

    /**
     * Returns the current reference count. Visible for testing.
     */
    int getRefCount() {
        return refCount.get();
    }

    /**
     * Closes all file handles and the doc values producer.
     */
    private void closeInternal() throws IOException {
        boolean success = false;
        try {
            IOUtils.close(dataIn);
            IOUtils.close(compositeDocValuesProducer);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(dataIn);
            }
            compositeIndexInputMap.clear();
            compositeIndexMetadataMap.clear();
            dataIn = null;
        }
    }

    /**
     * Called ONLY at refCount=0 with pendingDeletion set. Unprotects the sidecar files from
     * the {@link SidecarProtectedDirectory} and deletes them from the underlying directory.
     * No readers are active at this point, so file handles are already closed.
     */
    private void deleteFiles() {
        sidecarProtectedDirectory.unprotect(starTreeFiles);
        for (String file : starTreeFiles) {
            try {
                sidecarProtectedDirectory.getDelegate().deleteFile(file);
            } catch (Exception e) {
                logger.warn("Failed to delete sidecar file: " + file, e);
            }
        }
    }
}
