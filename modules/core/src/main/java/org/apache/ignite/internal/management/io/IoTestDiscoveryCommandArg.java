package org.apache.ignite.internal.management.io;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;

public class IoTestDiscoveryCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Argument(optional = true, description = "Payload size (bytes).")
    int payLoadSize = 100;

    /** */
    public int payLoadSize() {
        return payLoadSize;
    }

    /** */
    public void payLoadSize(int payLoadSize) {
        this.payLoadSize = payLoadSize;
    }
}
