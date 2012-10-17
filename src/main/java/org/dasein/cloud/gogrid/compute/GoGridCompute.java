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
