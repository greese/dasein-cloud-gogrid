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
package org.dasein.cloud.gogrid;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

/**
 * Implements data center services based on the GoGrid REST API.
 * <p>Created by George Reese: 10/13/12 3:28 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridDC implements DataCenterServices {
    static private final Logger logger = GoGrid.getLogger(GoGridDC.class);

    private GoGrid provider;

    GoGridDC(GoGrid provider) { this.provider = provider; }

    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            for( DataCenter dc : listDataCenters(r.getProviderRegionId()) ) {
                if( dc.getProviderDataCenterId().equals(providerDataCenterId) ) {
                    return dc;
                }
            }
        }
        return null;
    }

    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "data center";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "region";
    }

    @Override
    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            if( r.getProviderRegionId().equals(providerRegionId) ) {
                return r;
            }
        }
        return null;
    }

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        Region region = getRegion(providerRegionId);

        if( region == null ) {
            throw new CloudException("No such region: " + providerRegionId);
        }
        DataCenter dc = new DataCenter();
        dc.setActive(true);
        dc.setAvailable(true);
        dc.setName(region.getName() + "a");
        dc.setProviderDataCenterId(region.getProviderRegionId() + "a");
        dc.setRegionId(providerRegionId);
        return Collections.singletonList(dc);
    }

    static private HashMap<String,Collection<Region>> regionCache = new HashMap<String, Collection<Region>>();

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No region was set for this request");
        }
        Collection<Region> cached = regionCache.get(ctx.getEndpoint());

        if( cached != null ) {
            return cached;
        }
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray regionList = method.get(GoGridMethod.LOOKUP_LIST, new GoGridMethod.Param("lookup", "datacenter"));

        if( regionList == null ) {
            return Collections.emptyList();
        }

        // {"summary":{"total":3,"start":0,"numpages":0,"returned":3},
        // "status":"success",
        // "method":"/common/lookup/list",
        // "list":[{"id":1,"description":"US West 1 Datacenter","name":"US-West-1","object":"option"},{"id":2,"description":"US East 1 Datacenter","name":"US-East-1","object":"option"},{"id":3,"description":"EU-West-1 Datacenter","name":"EU-West-1","object":"option"}]}

        ArrayList<Region> regions = new ArrayList<Region>();

        for( int i=0; i<regionList.length(); i++ ) {
            try {
                Region r = toRegion(regionList.getJSONObject(i));

                if( r != null ) {
                    regions.add(r);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        regionCache.put(ctx.getEndpoint(), Collections.unmodifiableList(regions));
        return regions;
    }

    private @Nullable Region toRegion(@Nullable JSONObject r) throws CloudException, InternalException {
        if( r == null ) {
            return null;
        }

        Region region = new Region();

        region.setActive(true);
        region.setAvailable(true);
        region.setJurisdiction("US");
        try {
            if( r.has("id") ) {
                region.setProviderRegionId(r.getString("id"));
            }
            if( r.has("description") ) {
                region.setName(r.getString("description"));
            }
            if( r.has("name") ) {
                region.setName(r.getString("name"));
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to parse JSON from cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( region.getProviderRegionId() == null ) {
            return null;
        }
        if( region.getName() == null ) {
            region.setName(region.getProviderRegionId());
        }
        String n = region.getName();

        if( n.length() > 2 ) {
            region.setJurisdiction(n.substring(0, 2));
        }
        return region;
    }
}
