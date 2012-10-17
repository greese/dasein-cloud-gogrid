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

package org.dasein.cloud.gogrid;

import junit.framework.Test;

import org.dasein.cloud.test.ComprehensiveTestSuite;
import org.dasein.cloud.test.TestConfigurationException;

/**
 * Implements the junit integration tests so that Dasein Cloud can verify this implementation of the Dasein
 * Cloud API as it integrates with GoGrod.
 * <p>Created by George Reese: 10/13/12 1:41 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridTestSuite {
    static public Test suite() throws TestConfigurationException {
        return new ComprehensiveTestSuite(GoGrid.class);
    }
}
