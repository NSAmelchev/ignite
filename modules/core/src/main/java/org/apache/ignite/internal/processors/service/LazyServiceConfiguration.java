/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Lazy service configuration.
 */
public class LazyServiceConfiguration extends ServiceConfiguration {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] srvcBytes;

    /**
     * Default constructor.
     */
    public LazyServiceConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration.
     * @param srvcBytes Marshaller service.
     */
    public LazyServiceConfiguration(ServiceConfiguration cfg, byte[] srvcBytes) {
        assert cfg.getServiceClassName() != null : cfg;
        assert srvcBytes != null;

        name = cfg.getName();
        totalCnt = cfg.getTotalCount();
        maxPerNodeCnt = cfg.getMaxPerNodeCount();
        cacheName = cfg.getCacheName();
        affKey = cfg.getAffinityKey();
        nodeFilter = cfg.getNodeFilter();
        this.srvcBytes = srvcBytes;
    }

    /**
     * @return Service bytes.
     */
    public byte[] serviceBytes() {
        return srvcBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("RedundantIfStatement")
    @Override public boolean equalsIgnoreNodeFilter(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        LazyServiceConfiguration that = (LazyServiceConfiguration)o;

        if (maxPerNodeCnt != that.getMaxPerNodeCount())
            return false;

        if (totalCnt != that.getTotalCount())
            return false;

        if (affKey != null ? !affKey.equals(that.getAffinityKey()) : that.getAffinityKey() != null)
            return false;

        if (cacheName != null ? !cacheName.equals(that.getCacheName()) : that.getCacheName() != null)
            return false;

        if (name != null ? !name.equals(that.getName()) : that.getName() != null)
            return false;

        if (!F.eq(srvcClsName, that.getServiceClassName()))
            return false;

        if (!F.eq(prop, that.getServiceProperties()))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String nodeFilterCls = nodeFilter == null ? "" : nodeFilter.getClass().getSimpleName();

        return S.toString(LazyServiceConfiguration.class, this, "svcClsName", srvcClsName, "nodeFilterCls", nodeFilterCls);
    }
}
