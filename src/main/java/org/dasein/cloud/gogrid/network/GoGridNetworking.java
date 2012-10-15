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
package org.dasein.cloud.gogrid.network;

import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.network.ip.GoGridIPSupport;
import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.IpAddressSupport;

import javax.annotation.Nonnull;

/**
 * GoGrid IP, load balancer, and other networking services.
 * <p>Created by George Reese: 10/14/12 11:02 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridNetworking extends AbstractNetworkServices {
    private GoGrid provider;

    public GoGridNetworking(GoGrid provider) { this.provider = provider; }

    @Override
    public @Nonnull IpAddressSupport getIpAddressSupport() {
        return new GoGridIPSupport(provider);
    }
}
