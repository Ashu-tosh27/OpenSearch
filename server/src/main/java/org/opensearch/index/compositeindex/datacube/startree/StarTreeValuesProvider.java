/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;

import java.io.IOException;
import java.util.List;

/**
 * Standalone interface for providing star tree values from any source — either the native
 * codec pipeline ({@code Composite912DocValuesReader}) or sidecar files ({@code StarTreeSidecarReader}).
 * <p>
 * This interface is intentionally NOT extending {@code CompositeIndexReader} to avoid invasive
 * changes to the existing public API. Both {@code Composite912DocValuesReader} and
 * {@code StarTreeSidecarReader} implement this interface directly. {@code StarTreeQueryHelper}
 * checks for {@code CompositeIndexReader} first (existing native path), then falls back to
 * sidecar reader by segment name.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StarTreeValuesProvider {

    /**
     * Get list of composite index fields available from this provider.
     *
     * @return list of composite index field info objects
     */
    List<CompositeIndexFieldInfo> getCompositeIndexFields();

    /**
     * Get composite index values for the given field.
     *
     * @param fieldInfo the composite index field to retrieve values for
     * @return the composite index values (e.g., {@code StarTreeValues})
     * @throws IOException if an I/O error occurs reading the values
     */
    CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo fieldInfo) throws IOException;
}
