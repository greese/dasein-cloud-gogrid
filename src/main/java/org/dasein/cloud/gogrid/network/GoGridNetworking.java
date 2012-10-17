/**
 * Copyright (C) 2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.gogrid.network;

import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.network.ip.GoGridIPSupport;
import org.dasein.cloud.gogrid.network.lb.GoGridLBSupport;
import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.LoadBalancerSupport;

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

    @Override
    public @Nonnull LoadBalancerSupport getLoadBalancerSupport() {
        return new GoGridLBSupport(provider);
    }
}

