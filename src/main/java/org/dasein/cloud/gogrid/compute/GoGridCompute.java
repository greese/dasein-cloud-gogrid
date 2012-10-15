/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.gogrid.compute;

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.compute.image.GoGridImageSupport;
import org.dasein.cloud.gogrid.compute.server.GoGridServerSupport;

import javax.annotation.Nonnull;

/**
 * Implements the compute services supported in the GoGrid API.
 * <p>Created by George Reese: 10/13/12 8:16 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridCompute extends AbstractComputeServices {
    private GoGrid provider;

    public GoGridCompute(GoGrid provider) { this.provider = provider; }

    public @Nonnull GoGridImageSupport getImageSupport() {
        return new GoGridImageSupport(provider);
    }

    public @Nonnull GoGridServerSupport getVirtualMachineSupport() {
        return new GoGridServerSupport(provider);
    }
}
