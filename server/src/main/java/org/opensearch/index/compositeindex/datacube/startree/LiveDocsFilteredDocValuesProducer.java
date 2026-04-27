/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * A {@link DocValuesProducer} wrapper that filters out soft-deleted documents using a {@link Bits liveDocs}
 * bitset and remaps document IDs to a contiguous space (0 to numLiveDocs-1).
 * <p>
 * The sidecar star tree is a self-contained aggregation index. It presents a contiguous doc ID space
 * with no per-doc mapping back to the original segment. Ordinals in {@link SortedSetDocValues} and
 * {@link SortedDocValues} are internal to this filtered view and do NOT correspond to ordinals in the
 * original segment's doc values. There is no per-doc mapping back to the original segment.
 * <p>
 * The remapping works by building an array that maps each remapped doc ID (0, 1, 2, ...) to the
 * corresponding original doc ID. Each doc values wrapper translates {@code advanceExact(remappedId)}
 * to {@code delegate.advanceExact(remappedToOriginal[remappedId])}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LiveDocsFilteredDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final int[] remappedToOriginal;
    private final int numLiveDocs;

    /**
     * Creates a new LiveDocsFilteredDocValuesProducer.
     *
     * @param delegate the underlying doc values producer to wrap
     * @param liveDocs the bitset indicating which documents are live (not soft-deleted);
     *                 if null, all documents up to maxDoc are considered live
     * @param maxDoc   the total number of documents in the segment (including deleted)
     */
    public LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs, int maxDoc) {
        this.delegate = delegate;
        if (liveDocs == null) {
            // No deletes — identity mapping
            this.numLiveDocs = maxDoc;
            this.remappedToOriginal = new int[maxDoc];
            for (int i = 0; i < maxDoc; i++) {
                remappedToOriginal[i] = i;
            }
        } else {
            // Count live docs and build remapping
            int count = 0;
            for (int i = 0; i < maxDoc; i++) {
                if (liveDocs.get(i)) {
                    count++;
                }
            }
            this.numLiveDocs = count;
            this.remappedToOriginal = new int[count];
            int remapped = 0;
            for (int original = 0; original < maxDoc; original++) {
                if (liveDocs.get(original)) {
                    remappedToOriginal[remapped++] = original;
                }
            }
        }
    }

    /**
     * Returns the number of live (non-deleted) documents in the filtered view.
     */
    public int getNumLiveDocs() {
        return numLiveDocs;
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        SortedNumericDocValues inner = delegate.getSortedNumeric(field);
        return new FilteredSortedNumericDocValues(inner, remappedToOriginal, numLiveDocs);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericDocValues inner = delegate.getNumeric(field);
        return new FilteredNumericDocValues(inner, remappedToOriginal, numLiveDocs);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetDocValues inner = delegate.getSortedSet(field);
        return new FilteredSortedSetDocValues(inner, remappedToOriginal, numLiveDocs);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedDocValues inner = delegate.getSorted(field);
        return new FilteredSortedDocValues(inner, remappedToOriginal, numLiveDocs);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryDocValues inner = delegate.getBinary(field);
        return new FilteredBinaryDocValues(inner, remappedToOriginal, numLiveDocs);
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return delegate.getSkipper(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Filtered wrapper for {@link SortedNumericDocValues} that remaps doc IDs from a contiguous
     * space (0..numLiveDocs-1) to the original doc IDs using the remapping array.
     */
    private static class FilteredSortedNumericDocValues extends SortedNumericDocValues {

        private final SortedNumericDocValues inner;
        private final int[] remappedToOriginal;
        private final int numLiveDocs;
        private int remappedDocId = -1;

        FilteredSortedNumericDocValues(SortedNumericDocValues inner, int[] remappedToOriginal, int numLiveDocs) {
            this.inner = inner;
            this.remappedToOriginal = remappedToOriginal;
            this.numLiveDocs = numLiveDocs;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= numLiveDocs) {
                return false;
            }
            remappedDocId = target;
            return inner.advanceExact(remappedToOriginal[target]);
        }

        @Override
        public int docID() {
            return remappedDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            remappedDocId++;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= numLiveDocs) {
                remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            remappedDocId = target;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return numLiveDocs;
        }

        @Override
        public long nextValue() throws IOException {
            return inner.nextValue();
        }

        @Override
        public int docValueCount() {
            return inner.docValueCount();
        }
    }

    /**
     * Filtered wrapper for {@link NumericDocValues} that remaps doc IDs from a contiguous
     * space (0..numLiveDocs-1) to the original doc IDs using the remapping array.
     */
    private static class FilteredNumericDocValues extends NumericDocValues {

        private final NumericDocValues inner;
        private final int[] remappedToOriginal;
        private final int numLiveDocs;
        private int remappedDocId = -1;

        FilteredNumericDocValues(NumericDocValues inner, int[] remappedToOriginal, int numLiveDocs) {
            this.inner = inner;
            this.remappedToOriginal = remappedToOriginal;
            this.numLiveDocs = numLiveDocs;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= numLiveDocs) {
                return false;
            }
            remappedDocId = target;
            return inner.advanceExact(remappedToOriginal[target]);
        }

        @Override
        public int docID() {
            return remappedDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            remappedDocId++;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= numLiveDocs) {
                remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            remappedDocId = target;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return numLiveDocs;
        }

        @Override
        public long longValue() throws IOException {
            return inner.longValue();
        }
    }

    /**
     * Filtered wrapper for {@link SortedSetDocValues} that remaps doc IDs from a contiguous
     * space (0..numLiveDocs-1) to the original doc IDs using the remapping array.
     * <p>
     * Ordinals are internal to this filtered view and do NOT correspond to ordinals in the
     * original segment's doc values.
     */
    private static class FilteredSortedSetDocValues extends SortedSetDocValues {

        private final SortedSetDocValues inner;
        private final int[] remappedToOriginal;
        private final int numLiveDocs;
        private int remappedDocId = -1;

        FilteredSortedSetDocValues(SortedSetDocValues inner, int[] remappedToOriginal, int numLiveDocs) {
            this.inner = inner;
            this.remappedToOriginal = remappedToOriginal;
            this.numLiveDocs = numLiveDocs;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= numLiveDocs) {
                return false;
            }
            remappedDocId = target;
            return inner.advanceExact(remappedToOriginal[target]);
        }

        @Override
        public int docID() {
            return remappedDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            remappedDocId++;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= numLiveDocs) {
                remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            remappedDocId = target;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return numLiveDocs;
        }

        @Override
        public long nextOrd() throws IOException {
            return inner.nextOrd();
        }

        @Override
        public int docValueCount() {
            return inner.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return inner.lookupOrd(ord);
        }

        @Override
        public long getValueCount() {
            return inner.getValueCount();
        }
    }

    /**
     * Filtered wrapper for {@link SortedDocValues} that remaps doc IDs from a contiguous
     * space (0..numLiveDocs-1) to the original doc IDs using the remapping array.
     * <p>
     * Ordinals are internal to this filtered view and do NOT correspond to ordinals in the
     * original segment's doc values.
     */
    private static class FilteredSortedDocValues extends SortedDocValues {

        private final SortedDocValues inner;
        private final int[] remappedToOriginal;
        private final int numLiveDocs;
        private int remappedDocId = -1;

        FilteredSortedDocValues(SortedDocValues inner, int[] remappedToOriginal, int numLiveDocs) {
            this.inner = inner;
            this.remappedToOriginal = remappedToOriginal;
            this.numLiveDocs = numLiveDocs;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= numLiveDocs) {
                return false;
            }
            remappedDocId = target;
            return inner.advanceExact(remappedToOriginal[target]);
        }

        @Override
        public int docID() {
            return remappedDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            remappedDocId++;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= numLiveDocs) {
                remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            remappedDocId = target;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return numLiveDocs;
        }

        @Override
        public int ordValue() throws IOException {
            return inner.ordValue();
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            return inner.lookupOrd(ord);
        }

        @Override
        public int getValueCount() {
            return inner.getValueCount();
        }
    }

    /**
     * Filtered wrapper for {@link BinaryDocValues} that remaps doc IDs from a contiguous
     * space (0..numLiveDocs-1) to the original doc IDs using the remapping array.
     */
    private static class FilteredBinaryDocValues extends BinaryDocValues {

        private final BinaryDocValues inner;
        private final int[] remappedToOriginal;
        private final int numLiveDocs;
        private int remappedDocId = -1;

        FilteredBinaryDocValues(BinaryDocValues inner, int[] remappedToOriginal, int numLiveDocs) {
            this.inner = inner;
            this.remappedToOriginal = remappedToOriginal;
            this.numLiveDocs = numLiveDocs;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= numLiveDocs) {
                return false;
            }
            remappedDocId = target;
            return inner.advanceExact(remappedToOriginal[target]);
        }

        @Override
        public int docID() {
            return remappedDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            remappedDocId++;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= numLiveDocs) {
                remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            remappedDocId = target;
            while (remappedDocId < numLiveDocs) {
                if (inner.advanceExact(remappedToOriginal[remappedDocId])) {
                    return remappedDocId;
                }
                remappedDocId++;
            }
            remappedDocId = DocIdSetIterator.NO_MORE_DOCS;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return numLiveDocs;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            return inner.binaryValue();
        }
    }
}
